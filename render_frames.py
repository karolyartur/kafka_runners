import bpy
import sys
import os
import logging
import socket
import io

def parse_args(logger):
    '''
    Parse command line arguments (after -- that separates blender and Python arguments)
    '''
    argv = sys.argv
    if "--" not in argv:
        argv = []  # as if no args are passed
    else:
        argv = argv[argv.index("--") + 1:]  # get all args after "--"

    class Args():
        def __init__(self, frame_start=0, frame_num=1, gpu=False, output_dir='./tmp_render_out'):
            self.frame_start = frame_start
            self.frame_num = frame_num
            self.gpu = gpu
            self.output_dir = output_dir

    args = Args()

    try:
        for i,element in enumerate(argv):
            if element == '-fs':
                args.frame_start = int(argv[i+1])
            if element == '-fn':
                args.frame_num = int(argv[i+1])
            if element == '--gpu':
                args.gpu = True
            if element == '-o':
                args.output_dir = argv[i+1]
    except ValueError:
        logger.error('Invalid arguments passed to render_frames.py')
        exit(0)
    except IndexError:
        logger.error('Invalid arguments passed to render_frames.py')
        exit(0)
   
    return args

def create_logger():
    logger = logging.getLogger(__name__ + '@' + socket.gethostname())
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.ERROR)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(sh)
    return logger

def main():
    logger = create_logger()
    args=parse_args(logger)
    scene = bpy.context.scene
    if args.gpu:
        scene.cycles.device = 'GPU'
    start_frame = args.frame_start # Set start frame
    for i in range(args.frame_num):
        scene.render.filepath = os.path.join(args.output_dir, '')
        current_frame = start_frame + i
        scene.frame_set(current_frame) # Set frame
        scene.render.filepath = scene.render.frame_path(frame=current_frame)
        bpy.ops.render.render(write_still=True) # Render photorealistic image

if __name__=='__main__':
    main()
