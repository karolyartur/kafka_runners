import logging
import yaml
import importlib
import traceback
import os
import sys
import cv2
import numpy as np
import warnings

warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 

import tensorflow as tf
tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

class MRCNNTrainer():
    '''Class for training the Mask-RCNN model

    Use the train() method to train a model

    Args:
     - config_file_path (str): Path to the config file (containing network configuration)
     - train_path (str): Path to the folder containing the training images
     - valid_path (str): Path to the folder containing the validation images
    Keyword args:
     - cls_info (list of dicts): Has the form [{"name":"class1", "colors": [[1,1,1],[2,2,2], ...]}, {...}], colors have to be provided in HSV!
        A color ["rest"] means any other color apart from black. Defalt is None, in which case all the colors apart from black are matched against one single class
     - mrcnn_path (str): Path to the Mask-RCNN implementation, default is "../Mask_RCNN"
     - model_name (str): Name for the model, default is "model"
     - output_path (str): Path to the folder where the output of the training will be saved, default is "training_output"
     - logger (logging.logger object): A logger can be passed, if the MRCNNTrainer is used inside a class that has its own logger already (default is None which means a new logger will be created)
    '''
    def __init__(self, config_file_path, train_path, valid_path, cls_info=None, mrcnn_path='../Mask_RCNN', model_name='model', output_path='training_output', logger=None):
        '''Constructor for MRCNNTrainer
        '''
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__ + '.' + type(self).__name__)
        self.logger.setLevel(logging.DEBUG)
        self.config_file_path = config_file_path
        self.train_path = train_path
        self.valid_path = valid_path
        self.mrcnn_path = mrcnn_path
        self.model_name = model_name
        self.output_path = output_path

        try:
            # Import Mask-RCNN model definition
            sys.path.append(os.path.abspath(self.mrcnn_path))
            self.modellib = importlib.import_module('mrcnn.model')

            # Read the configs from the configuration file
            self._read_configs()

            # Prepare datasets
            self.train_dataset = self._prepare_dataset(self.train_path, cls_info)
            self.valid_dataset = self._prepare_dataset(self.valid_path, cls_info)

        except ModuleNotFoundError as e:
            # The Mask-RCNN implementation was not found
            self.logger.error('Could not find the Mask-RCNN implemntation at {}'.format(self.mrcnn_path))
            self.logger.error(traceback.format_exc())
            raise RuntimeError('Could not find the Mask-RCNN implemntation at {}'.format(self.mrcnn_path)) from e

        # Create model
        self.model = self.modellib.MaskRCNN(mode='training', config=self.config, model_dir=self.output_path)

    def _read_configs(self):
        '''Read configuration file and return a custom config object
        '''
        # Import Config class from Mask-RCNN implementation
        Config = importlib.import_module('mrcnn.config').Config

        # Define the custom Config class (for internal use only)
        class CustomConfig(Config):
            def __init__(i_self):
                i_self.NAME = self.model_name
                try:
                    with open(self.config_file_path, 'r') as f:
                        configs = yaml.safe_load(f)
                        if configs:
                            # Configs are loaded as a Python dictionary
                            for key, value in configs.items():
                                if hasattr(i_self, key):
                                    if type(value) is type(getattr(i_self, key)):
                                        setattr(i_self, key, value)  # Only overwrite attributes that already exist and have the correct type
                                    else:
                                        self.logger.warn('Attribute {} should have type {} but a value of type {} was provided in the config file. The default value will be used!'.format(key,type(getattr(i_self, key)),type(value)))
                    i_self.display()
                except AttributeError:
                    self.logger.warn('Invalid configuration file contents, ignoring configuration file {}!'.format(self.config_file_path))
                except FileNotFoundError as e:
                    self.logger.error('Could not find the configuration file at {}'.format(self.config_file_path))
                    self.logger.error(traceback.format_exc())
                    raise RuntimeError('Could not find the configuration file at {}'.format(self.config_file_path)) from e
                except yaml.YAMLError as e:
                    self.logger.error('Could not load the configuration file at {}. Invalid YAML!'.format(self.config_file_path))
                    self.logger.error(traceback.format_exc())
                    raise RuntimeError('Could not load the configuration file at {}. Invalid YAML!'.format(self.config_file_path)) from e
                super().__init__()
        
        # Create config
        self.config = CustomConfig()

    def _prepare_dataset(self, path, cls_info=None):
        '''Create, prepare and return custom dataset

        Args:
         - path (str): Path to the folder containing the images
        
        Keyword args:
         - cls_info (list of dicts): Has the form [{"name":"class1", "colors": [[1,1,1],[2,2,2], ...]}, {...}], colors have to be provided in HSV!
            A color ["rest"] means any other color apart from black. Defalt is None, in which case all the colors apart from black are matched against one single class
        '''
        # Import Dataset class from Mask-RCNN implementation
        Dataset = importlib.import_module('mrcnn.utils').Dataset

        # Define the custom Dataset class (for internal use only) 
        class CustomDataset(Dataset):
            def load_dataset(i_self, path, height, width, obj_name='object', class_info=None, annotation_naming_schema='###_annotation'):
                '''Load and store the information needed to use the dataset

                Args:
                 - path (str): Path to the folder containing the images
                 - height (int): The height of the input images (after reshaping, directly before being fed to the network)
                 - width (int): The width of the input images (after reshaping, directly before being fed to the network)
                Keyword args:
                 - obj_name (str): Name of the objects to be detected (Only used when there is only a single class apart from the background and no class_info is provided), default is "object"
                 - class_info (list of dicts): Has the form [{"name":"class1", "colors": [[1,1,1],[2,2,2], ...]}, {...}], colors have to be provided in HSV!
                    A color ["rest"] means any other color apart from black. Defalt is None, in which case obj_name is used as name and all the colors apart from black are matched against the class
                 - annotation_naming_schema (str): A naming schema for the annotations corresponding to the images. "###" in the schema will be replaced by the corresponding image filename. Default is "###_annotation"
                    Examples:
                     - 0000.png and 0000_annotation.png in the same folder -> annotation_naming_schema = "###_annotation"
                     - 0000.png and 0000_annotation.png are in different folders -> annotation_naming_schema = "../annotations/###_annotation"
                '''
                # Set class info
                if not class_info:
                    self.logger.warn('No class info provided. Assuming only one class!')
                    i_self.custom_class_info = [{"name":obj_name, "colors":[[0,0,255]]}]
                else:
                    i_self.custom_class_info = class_info

                # Check number of classes
                num_classes_wo_bg = self.config.NUM_CLASSES - 1
                if num_classes_wo_bg <= 0:
                    # Only background class, or invalid NUM_CLASSES in config file
                    self.logger.error('Not enough classes in configuration file! NUM_CLASSES must be 2 or more but it is {}'.format(self.config.NUM_CLASSES))
                    raise RuntimeError('Not enough classes in configuration file! NUM_CLASSES must be 2 or more but it is {}'.format(self.config.NUM_CLASSES))
                if len(i_self.custom_class_info) > num_classes_wo_bg:
                    # More classes in class info then in config
                    self.logger.warn('More classes in class info then in config. Classes will be ignored!')
                    i_self.custom_class_info = [i_self.custom_class_info[i] for i in range(num_classes_wo_bg)]
                elif num_classes_wo_bg > len(i_self.custom_class_info):
                    # More classes in config then in class info
                    self.logger.error('Not enough classes provided in class info: {}. Number of classes must be at least equal to {}'.format(i_self.custom_class_info, self.config.NUM_CLASSES-1))
                    raise RuntimeError('Not enough classes provided in class info: {}. Number of classes must be at least equal to {}'.format(i_self.custom_class_info, self.config.NUM_CLASSES-1))

                # Add classes
                class_names = []
                for index, class_descpition in enumerate(i_self.custom_class_info):
                    if class_descpition['name'] not in class_names:
                        i_self.add_class(self.model_name, index+1, class_descpition['name'])
                        class_names.append(class_descpition['name'])
                    else:
                        raise RuntimeError('Class names provided in class info must be unique, but "{}" appears multiple times.'.format(class_descpition['name']))
                
                # Add images
                try:
                    filenames = os.listdir(path)
                    filenames.sort()
                except FileNotFoundError as e:
                    self.logger.error('Invalid path provided for dataset: {}!'.format(path))
                    self.logger.error(traceback.format_exc())
                    raise RuntimeError('Invalid path provided for dataset: {}!'.format(path)) from e

                for index, filename in enumerate(filenames):
                    if os.path.isfile(os.path.join(path, filename)) and not annotation_naming_schema.replace('###', '') in filename and cv2.haveImageReader(os.path.join(path, filename)):
                        # Only add images if their path is valid, they are not annotations and they can be read by cv2
                        i_self.add_image(self.model_name, image_id=index, path=os.path.abspath(os.path.join(path, filename)), height=height, width=width, annotation_naming_schema=annotation_naming_schema)
            
            def load_image(i_self, image_id):
                '''Load image data into memory'''
                info = i_self.image_info[image_id]
                img = cv2.imread(info['path'], cv2.IMREAD_ANYCOLOR)
                img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
                img = cv2.resize(img, (info['width'], info['height']), interpolation= cv2.INTER_LINEAR)
                return img

            def image_reference(i_self, image_id):
                '''Get image info'''
                info = i_self.image_info[image_id]
                if info["source"] == self.model_name:
                    return info[self.model_name]
                else:
                    super(i_self.__class__).image_reference(i_self, image_id)
            
            def load_mask(i_self, image_id):
                '''Load mask annotation data into memory'''
                info = i_self.image_info[image_id]

                # Get filename and extension
                fname = '.'.join(os.path.split(info['path'])[-1].split('.')[:-1])
                extension = os.path.split(info['path'])[-1].split('.')[-1]

                # Construct filename for the annotation
                full_fname = info['annotation_naming_schema'].replace('###', fname) + '.' + extension
                mask = cv2.imread(os.path.join(os.path.split(info['path'])[0], full_fname), cv2.IMREAD_ANYCOLOR)
                hsv = cv2.cvtColor(mask, cv2.COLOR_BGR2HSV)

                # Get the bool matrices from the annotation
                bool_masks = i_self.mask_img_to_bool_matrix_hsv(hsv, (info['width'], info['height']))
                if bool_masks:
                    final_mask = bool_masks[0]
                    labels = np.ones(bool_masks[0].shape[2], dtype=np.int32)
                    for i in range(len(bool_masks)-1):
                        final_mask = np.append(final_mask,bool_masks[i+1],axis=2)
                        labels = np.append(labels, np.ones(bool_masks[i+1].shape[2], dtype=np.int32)*(i+2))
                    return final_mask, labels
                else:
                    self.logger.error('Converting masks to boolean matrices failed!')
                    raise RuntimeError('Converting masks to boolean matrices failed!')

            def mask_img_to_bool_matrix_hsv(i_self, mask, final_size=(576,324)):
                '''Covert a mask image to boolean matrices

                The colors in the mask images must be in HSV colorspace
                '''
                # Get all unique colors
                s = mask.shape
                mask_flat = np.resize(mask, (s[0]*s[1], s[2]))
                unique_colors = np.unique(mask_flat, axis=0)

                num_classes_wo_bg = self.config.NUM_CLASSES - 1
                masks = None
                if num_classes_wo_bg == 1:
                    # Only one class (without backround). Everything that is not black is considered to be an instance of the single class
                    for color in unique_colors:
                        if not all(color == [0,0,0]):
                            mask = np.reshape(np.all(mask_flat == color, axis=1), (s[0],s[1]))
                            inter = cv2.resize(mask.astype(np.uint8), final_size, interpolation= cv2.INTER_LINEAR)
                            mask = inter.astype(bool)
                            if not masks is None:
                                masks = np.append(masks, np.reshape(mask, (mask.shape[0],mask.shape[1],1)), axis=2)
                            else:
                                masks = np.reshape(mask, (mask.shape[0],mask.shape[1],1))
                    if masks is None:
                        inter = np.zeros((final_size[1],final_size[0],1))
                        masks = inter.astype(bool)
                    return [masks]
                elif num_classes_wo_bg > 1:
                    # Multiple classes
                    masks = [None for v in range(num_classes_wo_bg)]
                    for color in unique_colors:
                        if not all(color == [0,0,0]):
                            # Make a filter to check which classes "color" is associated with
                            class_filter = [list(color) in colors for colors in [classdesc['colors'] for classdesc in i_self.custom_class_info]]
                            corresponding_classes = [i for i, val in enumerate(class_filter) if val]
                            if not corresponding_classes:
                                # If unknown color check if there is a class with "rest" as color
                                corresponding_classes = [i for i, val in enumerate([['rest'] in colors for colors in [classdesc['colors'] for classdesc in i_self.custom_class_info]]) if val]
                            if len(corresponding_classes) >= 1:
                                if len(corresponding_classes) > 1:
                                    self.logger.warn('Multiple classes with same color ({}) in class info! Using first class with said color.'.format(color))
                                class_index = corresponding_classes[0]
                                mask = np.reshape(np.all(mask_flat == color, axis=1), (s[0],s[1]))
                                inter = cv2.resize(mask.astype(np.uint8), final_size, interpolation= cv2.INTER_LINEAR)
                                mask = inter.astype(bool)
                                if not masks[class_index] is None:
                                    masks[class_index] = np.append(masks[class_index], np.reshape(mask, (mask.shape[0],mask.shape[1],1)), axis=2)
                                else:
                                    masks[class_index] = np.reshape(mask, (mask.shape[0],mask.shape[1],1)) 
                            else:
                                self.logger.warn('Unknown color: {}. Objects with this color will be ignored! Add the color to the class info to include these objects.'.format(color))
                    for i, m in enumerate(masks):
                        # Crate empty masks for classes which do not have any instances in the image
                        if m is None:
                            inter = np.zeros((final_size[1],final_size[0],1))
                            masks[i] = inter.astype(bool)
                    return masks
                else:
                    # Only background class, or invalid NUM_CLASSES in config file
                    self.logger.error('Not enough classes in configuration file! NUM_CLASSES must be 2 or more but it is {}'.format(self.config.NUM_CLASSES))
                    raise RuntimeError('Not enough classes in configuration file! NUM_CLASSES must be 2 or more but it is {}'.format(self.config.NUM_CLASSES))


            def display_image(i_self, image_id):
                '''Display an image with cv2'''
                img = i_self.load_image(image_id)
                img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
                cv2.imshow('img', img)
                cv2.waitKey(0)

            def display_mask(i_self, image_id, ch=0):
                '''Display a mask with cv2'''
                masks, labels = i_self.load_mask(image_id)
                cv2.imshow('mask', masks[:,:,ch].astype(np.uint8)*255)
                cv2.waitKey(0)
        
        # Create, and prepare dataset
        dataset = CustomDataset()
        dataset.load_dataset(path, self.config.IMAGE_MIN_DIM, self.config.IMAGE_MAX_DIM, obj_name=self.config.NAME, class_info=cls_info)
        dataset.prepare()
        return dataset

    
    def train(self, *, init_with='coco', epochs=1, layers='heads'):
        '''Train the Mask-RCNN model

        Keyword args:
            - init_with (str): Weights to init the model with before training starts. Accepted values: "coco", "imagenet" and "last". Default is "coco"
            - epochs (int): Number of epoch to train the model for
            - layers (str): A selector for which layers to update during training. Default is "heads". For further info see the train() function in the Mask-RCNN implementation
        '''
        if init_with == "imagenet":
            self.model.load_weights(self.model.get_imagenet_weights(), by_name=True)
        elif init_with == "coco":
            coco_model_path = os.path.join("mask_rcnn_coco.h5")
            # Download COCO trained weights from Releases if needed
            if not os.path.exists(coco_model_path):
                try:
                    download_trained_weights = importlib.import_module('mrcnn.utils').download_trained_weights
                except ModuleNotFoundError as e:
                    self.logger.error('Could not find the Mask-RCNN implemntation at {}'.format(self.mrcnn_path))
                    self.logger.error(traceback.format_exc())
                    raise RuntimeError('Could not find the Mask-RCNN implemntation at {}'.format(self.mrcnn_path)) from e
                download_trained_weights(coco_model_path)
            self.model.load_weights(coco_model_path, by_name=True, exclude=["mrcnn_class_logits", "mrcnn_bbox_fc", "mrcnn_bbox", "mrcnn_mask"])
        elif init_with == "last":
            # Load the last trained model and continue training
            self.model.load_weights(self.model.find_last(), by_name=True)

        # Train the model
        self.model.train(self.train_dataset, self.valid_dataset, learning_rate=self.config.LEARNING_RATE, epochs=epochs, layers=layers)


if __name__=='__main__':
    trainer = MRCNNTrainer('config/config.yaml', 'train/renders', 'valid/renders', [{'name':'box', 'colors':[[0,255,255]]},{'name':'slide', 'colors':[['rest']]}])
    trainer.train(init_with='coco')