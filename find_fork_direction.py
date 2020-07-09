import argparse
import pandas as pd

ap = argparse.ArgumentParser()

# Add the arguments to the parser
ap.add_argument("-i", "--input", required=True, help="first operand")
ap.add_argument("-o", "--output", required=True, help="second operand")
args = vars(ap.parse_args())

revisions_list = pd.read_csv(args['input'])

