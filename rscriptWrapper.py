import os
import subprocess

rspt = os.path.join(os.getcwd(), 'myRScript.R')
subprocess.check_call(['Rscript', rspt], shell = False)
print "RSCIPT created"