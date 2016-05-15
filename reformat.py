import numpy as np
import re

def reformat(infile, outfile):
    with open(infile, 'r') as inFile:

        # Read in nodes
        n_nodes = inFile.readline()
        
        # Read in edges
        n_edges = inFile.readline()
        
        # Work through file filling in distances for each cell
        info = inFile.readlines()

    # Write graph to output file
    with open(outfile, 'w') as outFile:
        for i in range(0, int(n_nodes)):
            outFile.write(str(i)+' '+str(i)+'\n')

        outFile.write('#\n')
        for line in info:
            outFile.write(line.lstrip().replace('   ', ' ').replace('  ', ' '))

if __name__ == '__main__':

    reformat('largeEWD.txt', 'Trial/mil.txt')

    with open('Trial/output_mil.txt', 'r') as inFile:
        for line in inFile:
            line = line.strip()
            if line[:19] == "Distance to 999999:":
                print line
