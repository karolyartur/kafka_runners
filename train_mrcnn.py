import logging
import yaml
import os
import datetime
import torch
import torchvision
from datasets import InstanceSegmentationDataset
from utils import collate_fn
from engine import train_one_epoch, evaluate


class MRCNNTrainer():
    '''Class for training the Mask-RCNN model

    Use the train() method to train a model

    Args:
     - config_file_path (str): Path to the config file (containing network configuration)
     - train_path (str): Path to the folder containing the training images and annotations
     - valid_path (str): Path to the folder containing the validation images and annotations
    Keyword args:
     - model_name (str): Name for the model, default is "model"
     - output_path (str): Path to the folder where the output of the training will be saved, default is "training_output"
     - logger (logging.logger object): A logger can be passed, if the MRCNNTrainer is used inside a class that has its own logger already (default is None which means a new logger will be created)
    '''
    def __init__(self, s3, config_file_path, train_path, valid_path, model_name='model', output_path='training_output', logger=None, preprocess=True):
        '''Constructor for MRCNNTrainer
        '''
        self.s3 = s3

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__ + '.' + type(self).__name__)
            self.logger.setLevel(logging.DEBUG)
        self.train_path = train_path
        self.valid_path = valid_path
        self.model_name = model_name
        self.output_path = output_path

        try:
            # Read the configs from the configuration file
            self._read_configs(config_file_path)  # TODO: Add to configs: backbone, weights, anchors etc
        except FileNotFoundError as e:
            raise RuntimeError('Could not find the configuration file at {}'.format(config_file_path)) from e
        except yaml.YAMLError as e:
            raise RuntimeError('Could not load the configuration file at {}. Invalid YAML!'.format(config_file_path)) from e

        try:
            # Prepare datasets
            self.train_dataset = self._prepare_dataset(self.train_path, self.config.BATCH_SIZE, shuffle=True, num_workers=self.config.NUM_WORKERS, preprocess=preprocess)
            self.valid_dataset = self._prepare_dataset(self.valid_path, self.config.BATCH_SIZE, num_workers=self.config.NUM_WORKERS, preprocess=preprocess)
        except FileNotFoundError as e:
            raise RuntimeError('Could not prepare dataset!') from e


        # Create model
        self.model = torchvision.models.detection.maskrcnn_resnet50_fpn_v2(num_classes=self.config.NUM_CLASSES, box_detections_per_img=self.config.MAX_DETS)
        self.device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
        self.model.to(self.device)

    def _read_configs(self, config_file_path=None):
        '''Read configuration file and return a custom config object
        '''

        class Configs():

            # Class info
            NUM_CLASSES = 2

            # Training params
            BATCH_SIZE = 1
            NUM_EPOCHS = 1
            MAX_DETS = 100

            # Data loader params
            NUM_WORKERS = 0

            # SGD Optimizer params
            LEARNING_RATE = 0.005
            MOMENTUM = 0.9
            WEIGHT_DECAY = 0.0005

            # Learning rate scheduler params
            USE_LR_SCHEDULER = True
            LR_STEP_SIZE = 3
            LR_GAMMA = 0.1

            # Logging and output
            PRINT_FREQ = 1
            SAVE_FREQ = 1


            def __init__(i_self, config_file_path=None):
                if config_file_path:
                    i_self.config_file_path = config_file_path
                    try:
                        with self.s3.open(config_file_path, 'rb') as f:
                            configs = yaml.safe_load(f)
                            if configs:
                                # Configs are loaded as a Python dictionary
                                for key, value in configs.items():
                                    if hasattr(i_self, key):
                                        if type(value) is type(getattr(i_self, key)):
                                            setattr(i_self, key, value)  # Only overwrite attributes that already exist and have the correct type
                                        else:
                                            self.logger.warning('Attribute {} should have type {} but a value of type {} was provided in the config file. The default value will be used!'.format(key,type(getattr(i_self, key)),type(value)))
                    except AttributeError:
                        self.logger.warning('Invalid configuration file contents, ignoring configuration file {}!'.format(config_file_path))
                    except FileNotFoundError as e:
                        raise RuntimeError('Could not find the configuration file at {}'.format(config_file_path)) from e
                    except yaml.YAMLError as e:
                        raise RuntimeError('Could not load the configuration file at {}. Invalid YAML!'.format(config_file_path)) from e
                print(i_self)

            def __repr__(i_self):
                return 'Configs({}, {})'.format(i_self.config_file_path)

            def __str__(i_self):
                pretty_str = 'Configs: \n'
                for attribute in [a for a in dir(i_self) if not a.startswith('__') and not a.startswith('_') and not callable(getattr(i_self, a))]:
                    pretty_str += "\t{}: ".format(attribute) + str(getattr(i_self, attribute)) + '\n'
                return pretty_str
        
        # Create config
        self.config = Configs(config_file_path)
        self.logger.info('Loaded configs from "{}"'.format(config_file_path))

    def _prepare_dataset(self, path, batch_size, shuffle=False, num_workers=16, preprocess=True):
        '''Create, prepare and return dataset

        Args:
         - path (str): Path to the folder containing the images and annotations
         - batch_size (int): Number of samples to form a batch
         - shuffle (bool): Retrieve samples in random order
         - num_workers (int): Number of parallel workers for data loading, (default is 0, meaning no parallel workers)
        '''
        self.logger.info('Preparing dataset: "{}"'.format(path))
        dataset = InstanceSegmentationDataset(path, self.s3, logger=self.logger, preprocess=preprocess)
        data_loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers, collate_fn=collate_fn)
        return data_loader
    
    def train(self):
        '''Train the Mask-RCNN model
        '''
        params = [p for p in self.model.parameters() if p.requires_grad]
        optimizer = torch.optim.SGD(params, lr=self.config.LEARNING_RATE, momentum=self.config.MOMENTUM, weight_decay=self.config.WEIGHT_DECAY)
        lr_scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=self.config.LR_STEP_SIZE, gamma=self.config.LR_GAMMA)

        for epoch in range(self.config.NUM_EPOCHS):
            # train for one epoch, printing every iterations
            train_one_epoch(self.model, optimizer, self.train_dataset, self.device, epoch, print_freq=self.config.PRINT_FREQ)
            # update the learning rate
            if self.config.USE_LR_SCHEDULER:
                lr_scheduler.step()
            # evaluate on the test dataset
            evaluate(self.model, self.valid_dataset, device=self.device)
            if epoch % self.config.SAVE_FREQ == 0:
                with self.s3.open(os.path.join(self.output_path, '{}_maskrcnn_weights{}.pth'.format(self.model_name,str(datetime.datetime.now()).split('.')[0].replace(' ','_'))), 'wb') as f:
                    torch.save(self.model.state_dict(), f)


if __name__=='__main__':
    trainer = MRCNNTrainer("mrcnn_config.yaml", 'train', 'valid')
    trainer.train()
