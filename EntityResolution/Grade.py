import sys

gradeFile = open(sys.argv[1])

grades = [0 for xx in range(15)]

gradesdict = {


}
'''
numruns.test		numruns4.test		removeMiddle3.test	reversecopy2.test	swap2.test
numruns2.test		removeMiddle.test	removeMiddle4.test	reversecopy3.test	swap3.test
numruns3.test		removeMiddle2.test	reversecopy.test	swap.test		swap4.test
'''
total = 0


for i, line in enumerate(gradeFile.readlines()):
    if line.strip() == "0":
        grades[i] = 0
    else:
        grades[i] = 1


swapgrade = [a*b for a,b in zip(grades[11:15], [1, 2, 3, 2])]
numrunsgrade = [a*b for a,b in zip(grades[0:4], [1, 3, 3, 3])]
remmiddlegrade = [a*b for a,b in zip(grades[4:8], [1, 3, 3, 3])]
revcpgrade = [a*b for a,b in zip(grades[8:11], [1, 4, 5])]
total = sum(swapgrade) + sum(numrunsgrade) + sum(remmiddlegrade) + sum(revcpgrade) + 2

print "name:" + "\n" \
+ "loginid:"  + sys.argv[2] + "\n"\
+ "Assignment: CS 455 Extra Credit Assgt. Fall 2015" + "\n\n"\
+ "Total Score: " + str(total) + "/40" + "\n\n"\
+ "(" + str(total) + ") Correctness:" + "\n\n"\
        + "\t(" + str(sum(swapgrade) + 2) + "/10) swapLast2" + "\n"\
                + "\t\t(" + str(sum(swapgrade)) + "/8) pts on tests" + "\n"\
                        + "\t\t\t(" + str(swapgrade[0]) + "/1) empty list" + "\n"\
                        + "\t\t\t(" + str(swapgrade[1]) + "/2) one element in the list" + "\n"\
                        + "\t\t\t(" + str(swapgrade[2]) + "/3) two elements in the list" + "\n"\
                        + "\t\t\t(" + str(swapgrade[3]) + "/2) more than two elements in the list" + "\n"\
                + "\t\t(2/2) pts relinks nodes" + "\n"\
                        + "\t\t\t(no create, destroy nodes, or swap values)" + "\n\n"\
        + "\t(" + str(sum(numrunsgrade)) + "/10) numRuns" + "\n"\
                + "\t\t(" + str(numrunsgrade[0]) + "/1) empty list" + "\n"\
                + "\t\t(" + str(numrunsgrade[1]) + "/3) one element in the list" + "\n"\
                + "\t\t(" + str(numrunsgrade[2]) + "/3) two elements in the list" + "\n"\
                + "\t\t(" + str(numrunsgrade[3]) + "/3) more than two elements in the list" + "\n\n"\
        + "\t(" + str(sum(remmiddlegrade)) + "/10) removeMiddleElmt" + "\n"\
                + "\t\t(" + str(remmiddlegrade[0]) + "/1) empty list" + "\n"\
                + "\t\t(" + str(remmiddlegrade[1]) + "/3) one element in the list" + "\n"\
                + "\t\t(" + str(remmiddlegrade[2]) + "/3) two elements in the list" + "\n"\
                + "\t\t(" + str(remmiddlegrade[3]) + "/3) more than two elements in the list" + "\n\n"\
        + "\t(" + str(sum(revcpgrade)) + "/10) reverseCopy" + "\n"\
                + "\t\t(" + str(revcpgrade[0]) + "/1) empty list" + "\n"\
                + "\t\t(" + str(revcpgrade[1]) + "/4) one element in the list" + "\n"\
                + "\t\t(" + str(revcpgrade[2]) + "/5) more than one elements in the list" + "\n\n\n"\
+"Correctness comments:" + "\n\n"\
+"(-0) Resource usage (gets deducted from scores above):" + "\n\n"\
        +"\t(OK) uses O(n^2) time (or worse) [-2 per occurrence]" + "\n"\
        +"\t(OK) uses O(n) extra space (or worse) [-2 per occurrence]" + "\n"\
        +"\t(OK) leaks memory [-1 per occurrence]" + "\n"\
        +"\t(OK) can't evaluate resource usage because code is unreadable" + "\n\n"\
        +"\tResource comments:" + "\n\n"\
+ "Further Comments::" + "\n\n"\
        +"\t- For any questions, please email ghasemig@usc.edu"

