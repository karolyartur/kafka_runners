import argparse
import bpy
import bpy_extras
import os
import sys

def parse_args():
    '''
    Parse commans line arguments (after -- that separates blender and Python arguments)
    '''
    argv = sys.argv
    if "--" not in argv:
        argv = []  # as if no args are passed
    else:
        argv = argv[argv.index("--") + 1:]  # get all args after "--"

    # When --help or no args are given, print this help
    usage_text = (
        "Run blender with this script:"
        "  blender BLEND_FILE --python " + __file__ + " -- [options]"
    )


    # Argument parser
    parser = argparse.ArgumentParser(description=usage_text)

    parser.add_argument(
        "-fs", "--frame_start", dest="frame_start", type=int, required=False, default=0,
        help="Start frame (type: int, default: 0) Number of frame from which rendering starts",
    )

    parser.add_argument(
        "-fn", "--frame_num", dest="frame_num", type=int, required=False, default=1,
        help="Number of frames to render (type: int, default: 1)",
    )

    parser.add_argument(
        "-g", "--gpu", dest="gpu", action='store_true', default=False,
        help="Use GPU",
    )

    args = parser.parse_args(argv)
    
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