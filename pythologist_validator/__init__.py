import argparse


def do_inputs():
    parser=argparse.ArgumentParser(description="Check assumptions of image pipeline inputs.",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('input_format',help="Specify the type of input data you want to read")
    parser.add_argument('--panel',help="a json file with the panel definition",required=True)
    args = parser.parse_args()
    return args

def entry_point():
    args = do_inputs()

