import os
import torch
import numpy as np
from PIL import Image
import timeit

ALLOWED_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.npy']

def data_paths_from_root_path(imgs_path):
    '''Get image and annotation file paths from root path

    Args:
     - imgs_path (str/path): Path to the folder containing the images and annotations

    Returns:
     - data_paths (dict): A dictionary containing the paths to the images and corresponding annotations (format: {img_path:[annot1_path, annot2_path, ...], img2_path:[...], ...}) 

    '''
    # Filter the files from the folder
    files = [fname for fname in sorted(os.listdir(imgs_path)) if os.path.isfile(os.path.join(imgs_path, fname))]
    # Filter images (files, no "annotation" in their name and matching extension)
    imgs = [fname for fname in files if 'annotation' not in fname and os.path.splitext(fname)[1] in ALLOWED_EXTENSIONS]
    # Filter annotations (files, "annotation" in their name and matching extension)
    annots = [fname for fname in files if 'annotation' in fname and os.path.splitext(fname)[1] in ALLOWED_EXTENSIONS]

    # Match images and corresponding annotations
    data_paths = {}
    for img in imgs:
        corresponding_annots = [os.path.join(imgs_path, fname) for fname in annots if os.path.splitext(img)[0] in fname]
        if corresponding_annots:
            data_paths[os.path.join(imgs_path, img)] = corresponding_annots
    return data_paths


class InstanceSegmentationDataset(torch.utils.data.Dataset):
    '''Class for constructing COCO-style datasets for instance segmentation

    Args:
     - data_paths (dict): A dictionary containing the paths to the images and corresponding annotations (format: {img_path:[annot1_path, annot2_path, ...], img2_path:[...], ...})
     - transforms (function): A function for data augmentation f(images, targer) -> augmented_images,augmented_target
    '''
    def __init__(self, data_paths, transforms=None):
        self.imgs_path_list = [img_path for img_path in data_paths]
        self.masks_path_list = [annots[0] for img_path, annots in data_paths.items()]
        self.transforms = transforms

    def __getitem__(self, idx):
        '''Retrieve items from the dataset
        '''
        img_path = self.imgs_path_list[idx]
        mask_path = self.masks_path_list[idx]

        img = np.moveaxis(np.array(Image.open(img_path)), (2), (0))[:3]/255
        img = torch.as_tensor(img, dtype=torch.float32)
        mask = np.array(Image.open(mask_path))

        s = mask.shape
        mask_flat = np.reshape(mask, (s[0]*s[1], s[2]))
        obj_ids = np.unique(mask_flat, axis=0)[1:]
        binary_masks = np.reshape(np.all(mask_flat == obj_ids[:,None], axis=2), (-1,s[0],s[1]))
        boxes = []
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
            boxes.append(box)

        boxes = torch.as_tensor(boxes, dtype=torch.float32)
        labels = torch.ones((len(obj_ids),), dtype=torch.int64)
        masks = torch.as_tensor(binary_masks, dtype=torch.uint8)
        image_id = torch.tensor([idx])
        area = (boxes[:, 3] - boxes[:, 1]) * (boxes[:, 2] - boxes[:, 0])
        iscrowd = torch.zeros((len(obj_ids),), dtype=torch.int64)

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

def main():
    train_dataset = InstanceSegmentationDataset(data_paths_from_root_path('train'))
    iterator=iter(train_dataset)
    next(iterator)
    print(next(iterator))

if __name__=='__main__':
    print(timeit.timeit('main()', setup="from __main__ import main", number=1))