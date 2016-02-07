import sys
import threading
import socket
import struct
import time
import json
import base64
import fcntl
import geocron_protobuf.geocron_header_pb2 as geocron_header_pb2

"""
Basic class to store and print test information about a message received
"""
class MessageInfo:
	def __init__(self, message, time_received, msg_num, host, hops):
		#print message
		self.id = message['timestamp']
		self.received= time_received
		self.num = msg_num
		self.host = host
		self.hops = hops
		#print "Message " + str(self.num) + " from IP: " + self.host + " received in " + str(self.elapsed) + "\n"

	def get_statistics(self):
		return str(self.hops) + "," + str(self.received)

	def get_host(self):
		return self.host

	def get_received(self):
		return self.received

	def get_id(self):
		return str(self.id)

"""
Encapsulates MessageInfo objects of a particular IP. Provides file writing functionality
"""
class ServerObject:
	def __init__(self, host_, max_msgs_):
		print "Creating listening socket for host ip: " + host_ 
		self.host = host_
		self.message_dict = dict()
		self.messages_received = 0
		self.max_msgs_allowed = int(max_msgs_)

	def get_messages_received(self):
		return self.messages_received

	# """
	# Checks to see if all messages recorded has had its corresponding overlay recorded
	# """
	# def all_overlays_received(self):
	# 	print "Object dictionary has " + str(len(self.message_dict.keys())) + " objects."
	# 	if(self.messages_received != self.max_msgs_allowed):
	# 		return False
	# 	for k,v in self.message_dict.items():
	# 		if len(v) != 3: #if value hasn't received overlay message, it's length will be 1
	# 			if self.message_dict[k][0] == 1:
	# 				print "Deleting a message! Probably got lost!"
	# 				del self.message_dict[k]
	# 				self.messages_received -= 1
	# 			else:
	# 				self.message_dict[k][0] +=1
	# 			return False
	# 	return True

	"""
	When a new message is received, decide whether to add it as a new direct message 
	or add its overlay equivalent to the dictionary
	"""
	def process_message(self, message, header, time_received):
		if message['timestamp'] in self.message_dict.keys():
			self.message_dict[message['timestamp']].append(MessageInfo(message, time_received, self.messages_received, self.host, header.m_nHops))
			print "Found corresponding overlay"
			return 0
		else:
			self.messages_received += 1	
			self.message_dict[message['timestamp']] = [0, MessageInfo(message, time_received, self.messages_received, self.host, header.m_nHops)]
			return 1

	"""
	Build useful information from received and recorded messages and write to output file
	"""
	def write_messages_to_file(self, fo):
		#Attempt to grab file to write into (loop and try necessary or else error will be raised)
		while(True):
			try:
				fcntl.flock(fo, fcntl.LOCK_EX | fcntl.LOCK_NB)
				for k,v in self.message_dict.items():
					if(len(v) == 3):
						to_write = v[1].get_id() + "," +  v[1].get_host() + "," + v[1].get_statistics() + ","
						to_write += v[2].get_statistics() + ","
						to_write += str(v[2].get_received() - v[1].get_received()) + "\n"
						fo.write(to_write)
				fcntl.flock(fo, fcntl.LOCK_UN)
				return
			except IOError as e:
				time.sleep(0.1)
			else:
				time.sleep(0.1)	


"""
Manages list of IP's sending to the server. Configuration of maximum messages as well as IP's allowed here.
"""
class ServerManager:
	def __init__(self, fo, max_msgs):
		self.begin = time.time()
		self.host = "192.168.0.17"
		self.port = 11000
		self.host_ips = ["192.168.0.15", "192.168.0.21", "192.168.0.23", "192.168.0.25", "192.168.0.27", "192.168.0.29"]
		self.host_dict = dict()
		self.threads = []
		self.num_messages = 0
		self.lock = threading.Lock()
		self.max_msgs_allowed = int(max_msgs)
		self.fo = fo
		print "Starting Server Manager with " + str(len(self.host_ips)) + " IP's...\n" + "Maximum messages set to " + str(self.max_msgs_allowed) + "..."
		self.run() 
		self.end = time.time()
		print "Time elapsed is: " + str(self.end - self.begin)

	"""
	Kickstarts server manager. Called at object initialization
	"""
	def run(self):
		#Create a new ServerObject for each of the hosts in host_ips
		for host_ip in self.host_ips:
			new_thread = ServerObject(host_ip, self.max_msgs_allowed)
			self.host_dict[host_ip] = new_thread 
		#build server's listening socket
		client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		client_sock.bind((self.host, self.port))
		#whenever a message is received
		while (self.num_messages < self.max_msgs_allowed): #add multiplier?
			data, addr = client_sock.recvfrom(1024)
			current_time = time.time()
			self.build_background_thread(data, current_time) #process message in background	
		print "Attempted to read all messages. Writing statistics to file..."
		for k,v in self.host_dict.items():
			v.write_messages_to_file(self.fo)
		print "joining threads..."
		for t in self.threads:
			t.join()
		return

	"""
	Checks if all host objects had all of their overlays received
	"""
	def check_all_overlays_received(self):
		for k, host_obj in self.host_dict.items():
			if(not host_obj.all_overlays_received()):
				return False
		print "We got them."
		return True

	"""
	Builds background thread so when a message is received, processing is done in the background
	"""
	def build_background_thread(self, message, current_time):
		#build a thread to process a received message. Done concurrently to reduce process latency as much as possible
		thread = threading.Thread(target=self.process_messages, args=(message,current_time,))
		thread.daemon = True
		self.threads.append(thread)
		thread.start()

	"""
	Thread function called to process a received message.
	"""
	def process_messages(self, message, current_time):
		#decode json information to get the header
		decoded_information = json.loads(message)
		decoded_header = base64.b64decode(decoded_information['header'])
		#deserialize protobuf header to get origin ip
		deserialized_header = geocron_header_pb2.GeocronHeader()
		deserialized_header.ParseFromString(decoded_header)
		origin_ip = socket.inet_ntoa(struct.pack("!I",deserialized_header.m_origin))
		#check if 
		try:
			self.lock.acquire() 
			#num_messages is only incremented if the process message call acknowledges a new "direct" message
			self.num_messages += self.host_dict[origin_ip].process_message(decoded_information, deserialized_header, current_time)
		finally:
			self.lock.release()
		return

"""
Run main: python SCALE_test_server.py <file_to_write> <max_msgs>
"""
if __name__ == "__main__":
	file_name = sys.argv[1]
	max_msgs = sys.argv[2]
	fo = open(file_name, "w")
	process = ServerManager(fo, max_msgs)
	fo.close()

