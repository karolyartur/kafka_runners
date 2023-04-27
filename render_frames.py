import bpy
import sys

def parse_args():
    '''
    Parse command line arguments (after -- that separates blender and Python arguments)
    '''
    argv = sys.argv
    if "--" not in argv:
        argv = []  # as if no args are passed
    else:
        argv = argv[argv.index("--") + 1:]  # get all args after "--"

    class Args():
        def __init__(self, frame_start=0, frame_num=1, gpu=False):
            self.frame_start = frame_start
            self.frame_num = frame_num
            self.gpu = gpu

    args = Args()

    for element in argv:
        if '-fs' in element:
            args.frame_start = int(element.split(' ')[-1])
        if '-fn' in element:
            args.frame_num = int(element.split(' ')[-1])
        if '--gpu' in element:
            args.gpu = True
   
    return args

def main():
    args=parse_args()
    scene = bpy.context.scene
    if args.gpu:
        scene.cycles.device = 'GPU'
    start_frame = args.frame_start # Set start frame
    for i in range(args.frame_num):
        current_frame = start_frame + i
        scene.frame_set(current_frame) # Set frame
        scene.render.filepath = scene.render.frame_path(frame=current_frame)
        bpy.ops.render.render(write_still=True) # Render photorealistic image

if __name__=='__main__':
    main()