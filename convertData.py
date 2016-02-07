import numpy as np
import sys
import operator
import csv

"""
Reads a geocron csv test file and extracts its time difference attribute into a numpy array
"""
def get_time_data(file_to_read):
	np_array = []
	with open(file_to_read, 'r') as inp:
		csv_file = csv.reader(inp)
		for row in csv_file:
			np_array.append(float(row[6]))
	return np.array(np_array)

"""
Returns comma-separated string of file statistics. REMEMBER TO END WITH NEWLINE
csv format: <hops>,<minimum>,<maximum>,<median>,<mean>,<std_dev>,<variance>
"""
def get_file_statistics(raw):
	#important information: hops, minimum, maximum, average (median, mean), std. deviation/variance
	print raw
	return [np.amin(raw),np.amax(raw),np.median(raw),np.mean(raw),np.nanstd(raw),np.nanvar(raw)] 

def update_cstats(cstats, fstats):
	if (fstats[0] < cstats[0]):
		cstats[0] = fstats[0]
	if (fstats[1] > cstats[1]):
		cstats[1] = fstats[1]
	for i in range(2,6):
		cstats[i] += fstats[i]

def format_file_statistics(fdata, hops):
	return str(hops) + "," + str(fdata[0]) + "," + str(fdata[1]) + "," + str(fdata[2]) + "," + str(fdata[3]) + "," + str(fdata[4]) + "," + str(fdata[5]) + "\n"

def form_file_string(hops, trials, index):
	return "Data" + str(hops) + "Hops" + str(trials) + "." + str(index) + ".csv"

def run(input_dir, num_trials, num_docs):
	output_file = str(num_trials)+ "_statistics.csv"
	raw_data = dict()
	all_stats = []
	with open(output_file, 'w') as outfile:
		#read all files in a loop
		for hops in range(1, 6): #hops range from 1-5
			raw_data[str(hops)] = []
			cstats = [0 for i in range(0,6)]
			cstats[0] = 999 #set to higher number so it decrements 	
			for i in range(0,num_docs):
				file_to_read = input_dir + "/" + form_file_string(hops, num_trials, i)
				raw = get_time_data(file_to_read)
				#raw_data[str(hops)].append(raw)  #not used right now
				fstats = get_file_statistics(raw)
				update_cstats(cstats, fstats)
				outfile.write(format_file_statistics(fstats, hops))
			all_stats.append(str(hops)+","+str(cstats[0])+","+str(cstats[1])+","+str(cstats[2]/num_docs)
				+","+str(cstats[3]/num_docs)+","+str(cstats[4]/num_docs)+","+str(cstats[5]/num_docs)+"\n")
		outfile.write("Compiled statistics,min,max,median,mean,std,var\n")
		for line in all_stats:
			outfile.write(line)





"""
Usage: python convertData.py <folder_of_data>p <trials> <num_documents>
"""
if __name__ == "__main__":
	input_dir = sys.argv[1] #directory where input files belong
	num_trials = int(sys.argv[2])
	num_docs = int(sys.argv[3])
	run(input_dir, num_trials, num_docs)


