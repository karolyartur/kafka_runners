import logging
import torch
import torchvision
import numpy as np
from PIL import Image

class MRCNNInference():
    '''Class for making predictions with the Mask-RCNN model

    Use the predict() method to train a model

    Args:
     - model_weights_path (str): Path to the model weights file (in local filesystem)
     - max_dets (int): Maximum number of objects to predict per image
    Keyword args:
     - logger (logging.logger object): A logger can be passed, if the MRCNNInference is used inside a class that has its own logger already (default is None which means a new logger will be created)
    '''
    def __init__(self, s3, model_weights_path, max_dets=100, logger = None):
        '''Constructor for MRCNNTrainer
        '''
        self.s3 = s3
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__ + '.' + type(self).__name__)
            self.logger.setLevel(logging.DEBUG)
        self.device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')

        try:
            checkpoint = torch.load(model_weights_path, map_location=self.device)
        except FileNotFoundError as e:
            logger.error('Could not load model weights from path :"{}"'.format(model_weights_path))
            raise RuntimeError('Could not load model weights from path :"{}"'.format(model_weights_path)) from e

        last_key = list(checkpoint)[-1]
        num_classes = list(checkpoint[last_key].size())[0]

        try:
            self.model = torchvision.models.detection.maskrcnn_resnet50_fpn_v2(num_classes=num_classes, box_detections_per_img=max_dets)
        except RuntimeError as e:
            self.logger.error('Could not create Mask-RCNN model.')
            raise RuntimeError('Could not create Mask-RCNN model.') from e

        self.model.to(self.device)
        self.model.load_state_dict(checkpoint)
        self.model.eval()


    def predict(self, img_paths):
        '''Make predictions with the Mask-RCNN model

        Args:
         - img_paths (list[str]): Strings describing the location of the image in the MinIO storage. Format: BUCKET_NAME/PATH/TO/IMAGE

        Returns:
         - preds tuple[p, masks]: The predictions of the model for the given image, where "p" is a dictionary containing predicted boxes, labels and scores and "masks" is a numpy array of segmentation masks
        '''
        for i, img_path in enumerate(img_paths):
            with self.s3.open(img_path, 'rb') as f:
                img = np.array(Image.open(f))
                img = np.moveaxis(img, (2), (0))[:3]/255
            if i == 0:
                imgs = torch.as_tensor(np.expand_dims(img,axis=0), dtype=torch.float32).to(self.device)
            else:
                img = torch.as_tensor(np.expand_dims(img,axis=0), dtype=torch.float32).to(self.device)
                imgs = torch.cat((imgs, img), 0)
        preds = self.model(imgs)
        boxes = [{k:v.tolist() for k,v in pred.items() if k !='masks'} for pred in preds]
        masks = [pred['masks'].cpu().detach().numpy() for pred in preds]
        shapes = [m.shape for m in masks]
        masks = [np.reshape(m, (s[0],s[2],s[3])) for m,s in zip(masks,shapes)]
        return boxes, masks


if __name__=='__main__':
    from minio_client import MinioClient
    minio_client = MinioClient()
    mrcnn = MRCNNInference(minio_client._s3, "mushroom__0000_maskrcnn_weights2023-08-01_00_38_35.pth")
    print(mrcnn.predict('minio.python.api.test/dataset_test/0000.png'))