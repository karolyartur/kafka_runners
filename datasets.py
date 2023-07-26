import os
import torch
import numpy as np
from PIL import Image
import timeit
import logging

ALLOWED_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.npy']


class InstanceSegmentationDataset(torch.utils.data.Dataset):
    '''Class for constructing COCO-style datasets for instance segmentation

    Args:
     - imgs_path (str): A string describing the location of the dataset in the MinIO storage. Format: BUCKET_NAME/PATH/TO/FOLDER
     - transforms (function): A function for data augmentation f(images, target) -> augmented_images,augmented_target
    '''
    def __init__(self, imgs_path, s3, transforms=None, logger=None):

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__ + '.' + type(self).__name__)
            self.logger.setLevel(logging.DEBUG)

        self.imgs_path = imgs_path
        self.s3 = s3
        imgs_path_contents = self.s3.ls(self.imgs_path)

        # Filter the files from the folder
        files = [fpath for fpath in sorted(imgs_path_contents) if self.s3.isfile(fpath)]
        # Filter images (files, no "annotation" in their name and matching extension)
        imgs = [fpath for fpath in files if 'annotation' not in os.path.split(fpath)[-1] and os.path.splitext(fpath)[1] in ALLOWED_EXTENSIONS]
        # Filter annotations (files, "annotation" in their name and matching extension)
        annots = [fpath for fpath in files if 'annotation' in os.path.split(fpath)[-1] and os.path.splitext(fpath)[1] in ALLOWED_EXTENSIONS]

        preprocessed = self.s3.isfile(os.path.join(self.imgs_path, 'masks.npz')) and self.s3.isfile(os.path.join(self.imgs_path, 'boxes.npz'))
        if not preprocessed:
            self.logger.info('Dataset is not preprocessed yet! Starting preprocessing.')
            self.masks, self.boxes = self.preprocess_dataset(annots)
        else:
            self.logger.info('Opening preprocessed annotations: "{}" and "{}"'.format(os.path.join(self.imgs_path, 'masks.npz'), os.path.join(self.imgs_path, 'boxes.npz')))
            with self.s3.open(os.path.join(self.imgs_path, 'masks.npz'),'rb') as f:
                masks = np.load(f)
                self.masks = {}
                for k in masks.keys():
                    self.masks[k] = masks[k]
            with self.s3.open(os.path.join(self.imgs_path, 'boxes.npz'),'rb') as f:
                boxes = np.load(f)
                self.boxes = {}
                for k in boxes.keys():
                    self.boxes[k] = boxes[k]
        self.imgs_path_list = imgs
        self.transforms = transforms

    def __getitem__(self, idx):
        '''Retrieve items from the dataset
        '''
        img_path = self.imgs_path_list[idx]
        img_id = os.path.splitext(os.path.split(img_path)[-1])[0]

        with self.s3.open(img_path, 'rb') as f:
            img = np.array(Image.open(f))
        s = img.shape
        img = np.moveaxis(img, (2), (0))[:3]/255
        img = torch.as_tensor(img, dtype=torch.float32)
        binary_masks = np.reshape(np.unpackbits(self.masks[img_id]), (-1,s[0],s[1]))
        boxes = self.boxes[img_id]

        boxes = torch.as_tensor(boxes, dtype=torch.float32)
        labels = torch.ones((len(binary_masks),), dtype=torch.int64)
        masks = torch.as_tensor(binary_masks, dtype=torch.uint8)
        image_id = torch.tensor([idx])
        area = (boxes[:, 3] - boxes[:, 1]) * (boxes[:, 2] - boxes[:, 0])
        iscrowd = torch.zeros((len(binary_masks),), dtype=torch.int64)

        target = {}
        target["boxes"] = boxes
        target["labels"] = labels
        target["masks"] = masks
        target["image_id"] = image_id
        target["area"] = area
        target["iscrowd"] = iscrowd

        if self.transforms is not None:
            img, target = self.transforms(img, target)

        return img, target


    def __len__(self):
        return len(self.imgs_path_list)


    def preprocess_dataset(self, annots):
        masks = {}
        boxes = {}
        for annot in annots:
            img_id = os.path.splitext(os.path.split(annot)[-1])[0].replace('_annotation','')
            with self.s3.open(annot, 'rb') as f:
                mask = np.array(Image.open(f))
            s = mask.shape
            mask_flat = np.reshape(mask, (s[0]*s[1], s[2]))
            obj_ids = np.unique(mask_flat, axis=0)[1:]
            binary_masks = np.reshape(np.all(mask_flat == obj_ids[:,None], axis=2), (-1,s[0],s[1]))

            bboxes = []
            for binary_mask in binary_masks:
                pos = np.nonzero(binary_mask)
                box = [np.min(pos[1]), np.min(pos[0]), np.max(pos[1]), np.max(pos[0])]  # xmin, ymin, xmax, ymax
                if box[0]==box[2] and box[2] < s[0]-1:
                    box[2] += 1
                elif box[0]==box[2] and box[0] > 0:
                    box[0] -= 1
                if box[1]==box[3] and box[3] < s[1]-1:
                    box[3] += 1
                elif box[1]==box[3] and box[1] > 0:
                    box[1] -= 1
                bboxes.append(np.array(box))
            masks[img_id] = np.packbits(binary_masks)
            boxes[img_id] = np.array(bboxes)

        with self.s3.open(os.path.join(self.imgs_path, 'masks.npz'),'wb') as f:
            np.savez_compressed(f, **masks)
        with self.s3.open(os.path.join(self.imgs_path, 'boxes.npz'),'wb') as f:
            np.savez_compressed(f, **boxes)
        self.logger.info('Preprocessing finished.')
        return masks, boxes

def main():
    train_dataset = InstanceSegmentationDataset('train')
    iterator=iter(train_dataset)
    next(iterator)
    print(next(iterator))

if __name__=='__main__':
    print(timeit.timeit('main()', setup="from __main__ import main", number=1))