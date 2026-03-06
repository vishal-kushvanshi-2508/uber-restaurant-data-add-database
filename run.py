import subprocess

def total(total,parts):
    range_of_files=int(total/parts)
    for i in range(1,total,range_of_files):
        start = i
        end = min(i + range_of_files, total + 1)
        command = "start cmd /k python main.py " + str(start) + " " + str(end)
        with open('run.bat', 'a') as f:
            f.write(command+'\n')
        # subprocess.run(command)


print("run file exiqure ")
total(122888,10)