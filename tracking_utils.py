import pymongo
import datetime
import itertools
import numpy as np
from scipy.optimize import linear_sum_assignment

import json


def compute_iou(bboxes1: 'list[list[int]]', bboxes2: 'list[list[int]]') -> 'tuple[list[list[int]]]':
    bboxes1 = np.array(bboxes1)
    bboxes2 = np.array(bboxes2)
    x11, y11, x12, y12 = np.split(bboxes1, 4, axis=1)
    x21, y21, x22, y22 = np.split(bboxes2, 4, axis=1)
    xA = np.maximum(x11, np.transpose(x21))
    yA = np.maximum(y11, np.transpose(y21))
    xB = np.minimum(x12, np.transpose(x22))
    yB = np.minimum(y12, np.transpose(y22))
    interArea = np.maximum((xB - xA + 1), 0) * np.maximum((yB - yA + 1), 0)
    boxAArea = (x12 - x11 + 1) * (y12 - y11 + 1)
    boxBArea = (x22 - x21 + 1) * (y22 - y21 + 1)
    iou = interArea / (boxAArea + np.transpose(boxBArea) - interArea)
    return iou, interArea


class Mushroom():
    '''
    Class for instances. It has an unique ID, box values (x_min, y_min, x_max, y_max), size in pixels, birth and pick date.
    '''
    def __init__(self, id: int, box: 'list[int]', birth_date: str=None, pick_date: str=None):
        self.id = id    # tracker's id 
        self.box = box  # list to store the coordinates for a bounding box (x_min, y_min, x_max, y_max)
        if birth_date:
            self.birth_date = self.str_date_to_datetime(birth_date)    # datetime of birth
        if pick_date:
            self.pick_date = self.str_date_to_datetime(pick_date)    # datetime of pick
        self.consecutive_matches: int = 0  # Number of consecutive matches
        self.consecutive_non_matches: int = 0  # Number of non-consecutive matches

    def str_date_to_datetime(self, date:str) -> datetime.datetime:
        '''
        Convert input date strings to datetime objects
        '''
        return datetime.datetime.strptime(date, '%Y_%m_%d_%H%M')
    
    def update_match(self, box: 'list[int]') -> None:
        '''
        Update mushroom instance when successfully matched with a detection.
        '''
        self.box = box
        self.consecutive_matches += 1
        self.consecutive_non_matches = 0

    def update_non_match(self) -> None:
        '''
        Update mushroom instance when not matched with any detection (picked).
        '''
        self.consecutive_non_matches += 1
        self.consecutive_matches = 0

    def pick(self, pick_date: str):
        '''
        Add a pick date to the mushroom instance and return it
        '''
        self.pick_date = self.str_date_to_datetime(pick_date)
        return self

    @property
    def size(self) -> int:
        '''
        Size of the instance in pixel units
        '''
        x_min,y_min,x_max,y_max = self.box
        return (x_max-x_min)*(y_max-y_min)


class Tracker():
    '''
    Mushroom tracker
    '''
    def __init__(self, iou_threshold: float=0.5, max_candidate_age: int=2, max_lost_age: int=4, start_id: int=0, filter: 'list[int]'= None):
        self.current_id = start_id
        self.iou_threshold = iou_threshold
        self.max_candidate_age = max_candidate_age
        self.max_lost_age = max_lost_age
        self.tracked_instances: 'list[Mushroom]' = []
        self.candidate_instances: 'list[Mushroom]' = []
        self.picked_instances: 'list[Mushroom]' = []
        self.filter_box = filter

    def _update_candidate_instances(self) -> None:
        '''
        Update the list of candidate instances after mathing with detections.
        '''
        # Add candidate_instances with consecutive_matches >= max_candidate_age to tracked_instances
        self.tracked_instances.extend([i for i in self.candidate_instances if i.consecutive_matches>=self.max_candidate_age])
        # Remove these instances from candidate_instances
        self.candidate_instances = [i for i in self.candidate_instances if i.consecutive_matches<self.max_candidate_age]

    def _update_picked_instances(self, current_date: str) -> None:
        '''
        Update the list of picked instances after mathing with detections.
        '''
        # Add tracked_instances with consecutive_non_matches >= max_lost_age to picked_instances
        self.picked_instances.extend([i.pick(current_date) for i in self.tracked_instances if i.consecutive_non_matches>=self.max_lost_age])
        # Remove these instances from tracked _instances
        self.tracked_instances = [i for i in self.tracked_instances if i.consecutive_non_matches<self.max_lost_age]

    def _update(self, current_date: str) -> None:
        '''
        Update tracker
        '''
        self._update_candidate_instances()
        self._update_picked_instances(current_date)

    def _filter(self, box:'list[int]')->bool:
        '''
        Filter boxes before adding them to candidates
        '''
        interArea = [[1]]
        if self.filter_box:
            _,interArea = compute_iou([box],[self.filter_box])
        return interArea[0][0] > 0

    def add_candidate_instance(self, box: 'list[int]', current_date: str):
        '''
        Add a mushroom instance to the candidate_instances list
        '''
        if self._filter(box):
            self.candidate_instances.append(Mushroom(self.current_id, box, current_date))
            self.current_id += 1

    def match(self, detections: 'list[list[int]]', current_date: str) -> None:
        '''
        Match tracked instances with detections
        '''
        iou,_ = compute_iou(detections, [i.box for i in itertools.chain(self.candidate_instances, self.tracked_instances)])
        detect_ind, can_track_ind = linear_sum_assignment(iou, maximize=True)

        # Remove matches with iou < iou_threshold
        remove_matches= [j for j, (di,cti) in enumerate(zip(detect_ind, can_track_ind)) if iou[di][cti]<self.iou_threshold]
        detect_ind = [i for j, i in enumerate(detect_ind) if j not in remove_matches]
        can_track_ind = [i for j, i in enumerate(can_track_ind) if j not in remove_matches]

        # Update matched tracked instances
        for di,cti in zip(detect_ind, can_track_ind):
            if cti < len(self.candidate_instances):
                self.candidate_instances[cti].update_match(detections[di])
            else:
                self.tracked_instances[cti-len(self.candidate_instances)].update_match(detections[di])

        # Update unmatched tracked instances
        for cti in [i for i in range(len(self.candidate_instances) + len(self.tracked_instances)) if i not in can_track_ind]:
            if cti < len(self.candidate_instances):
                self.candidate_instances[cti].update_non_match()
            else:
                self.tracked_instances[cti-len(self.candidate_instances)].update_non_match()

        # Add unmatched detections to candidate instances
        for di in [i for i in range(len(detections)) if i not in detect_ind]:
            self.add_candidate_instance(detections[di],current_date)
        
        # Update tracked instance lists
        self._update(current_date)

    def export(self, img_id: str) -> dict:
        export = {}
        export['img_id'] = img_id
        export['candidate'] = [{'id':i.id, 'size':i.size, 'box':i.box} for i in self.candidate_instances]
        export['tracked'] = [{'id':i.id, 'size':i.size, 'box':i.box} for i in self.tracked_instances]
        export['picked'] = [{'id':i.id, 'size':i.size, 'box':i.box} for i in self.picked_instances]
        return export


if __name__=='__main__':

    myclient = pymongo.MongoClient("mongodb://kafkarunneruser:kafkapass12345@10.8.8.224:27017/?authMechanism=DEFAULT&authSource=mushroom_monitor")

    db = myclient["mushroom_monitor"]


    DATE_START = '2023_11_01'
    DATE_END = '2023_11_17'

    IOU_THRD = 0.5
    MAX_CANDIDATE_AGE = 2
    MAX_LOST_AGE = 4
    FILTER = [680,692,2600,1772]

    myquery = {"_id": {"$gte":DATE_START, "$lt":DATE_END}}

    tracker = Tracker(IOU_THRD, MAX_CANDIDATE_AGE, MAX_LOST_AGE, filter=FILTER)

    tracking_result = []
    timestep = 0
    for x in db['mushroom_0000_maskrcnn_weights_2023_08_01_0038'].find(myquery, {"_id":1,"predictions":1}).limit(100):
        # print(x['_id'])
        detections = x['predictions']['boxes']
        if timestep == 0:
            for detection in detections:
                tracker.add_candidate_instance(detection, x['_id'])
        else:
            tracker.match(detections,x['_id'])
            res = tracker.export(x['_id'])
            print(res['tracked'])
            tracking_result.append(res)
        timestep += 1

    with open('test.json','w') as f:
        f.write(json.dumps(tracking_result))