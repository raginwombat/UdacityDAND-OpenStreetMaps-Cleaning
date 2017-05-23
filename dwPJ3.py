import csv
from bs4 import BeautifulSoup
from pymongo import MongoClient
import xml.etree.cElementTree as ET
import re
import pprint
#Get stats on exec time
import time

start_time = time.time()


#Setup global vars for local db
DBNAME ='test'
client = MongoClient('127.0.0.1', 27017)
db = client.dwp3
cur = db[DBNAME]


#Setup variables for file processing and various audit parking constructs
OSMFILE = 'orlando_florida.osm'
ERRLOG = 'error_'+OSMFILE
expected_way_endings = ['Street', 'Avenue', 'Boulevard', 'Drive', 'Court',\
						'Play', 'Way', 'Pike', 'Parkway','Road', 'Trail', \
						'Turnpike', 'Loop', 'Circle', 'Lane']
unexpected_way_endings = set()
street_ending_re = re.compile(r'\b\S+\.?$')

street_mappings = {'St': 'Street',
		            'St.': 'Street',
		            'Ave':'Avenue',
		            'Rd.': 'Road',
		            'Blvd': 'Boulevard',
		            'Ct':' Court'}


name_fixes = {  'H.S.': 'High School',
                'HS': 'High School'}
records_with_errors = set()

def check_street_endings():
	'''
	This is an audit method to check for odd street name endings
	If a strange ending and  is already mapps it replaces the value
	If it can't be mapped the record is added to an error dict 
	'''

	for record in  cur.find({'addr': {'$exists': True }}):
		street_ending_re_search = street_ending_re.search(record['addr'])
		if street_ending_re_search:
			street_type = street_ending_re_search.group()
			if street_type not in expected_way_endings:
				unexpected_way_endings.add(street_type)
				fix_street_name(record)
	   		else:
	   			records_with_errors.add(record['_id'])
	print unexpected_way_endings


def fix_street_name(node):
	'''
		This menthod fixes the street endings that aren't expected and replaces them
		with the dict values contained in street_mappings
		Future dev: abstract this to a generic update fuction. Let it handle all updates to map values and dicsts
		Use a set w/ the object id's to track itesms with issues
	'''
	street_ending_re_search = street_ending_re.search(node['addr']).group()
	if street_ending_re_search in street_mappings:
		#replace the incorrect entry identifyed with the dict mapping
		corrrected_addr = re.sub(street_ending_re, street_mappings[street_ending_re_search], node['addr']) 
		#update db with the corrected value
		cur.update( {'_id':node['_id']}, \
						{'$set': {'addr':corrrected_addr, 'corrected':True }}, \
						upsert=False)

		

def is_highway(node):
	#deprecated/unused
	re.compile(r'')


def extract_and_write_nodes_to_db(filename):
	err_street_endings = dict()
	street = {}
	''' Document Structure
			'name': Name of element,
			'long': Longitude
			'lat" : lattitude'
			'addr': Address of Node if applicable,
			'type': type of tag way|node},
			'username':  Name of user who edited element
			'user id':  Id of user who edited element
			'loc_class':  Additional location information support furthuer cleaning
	'''

	with open(filename, 'r') as osmfile:
		#Parse through the file interatively
		#Debug option, limit processing
		i=0
		for event, elem in ET.iterparse(filename, events=('start',)):
				#Filter tags for only node or ways tha will have tag elements
				if elem.tag =='node' or elem.tag =='way':
					#Add type to dict to write to db
					street['type'] = elem.tag
					#Add locs to dict to write to db
					if elem.tag =='node':
						street['lat'] = get_attribute( elem, 'lat', ERRLOG)
						street['long'] = get_attribute( elem, 'lon', ERRLOG)
					for tag in elem.iter('tag'):
						#Get name of the element
						if tag.attrib['k'] == 'name':
							street['name'] = tag.attrib['v']
						#Get street names for each tag
						if tag.attrib['k']  =="addr:street":
							street['addr'] = tag.attrib['v']
						if tag.attrib['k'] == 'amenity':
							street['loc_class'] = tag.attrib['v']
					street['userid'] =get_attribute( elem, 'uid', ERRLOG)
					street['username'] = get_attribute( elem, 'user', ERRLOG)
				
				if bool(street):
					cur.insert(street)

				street.clear()
				
				
				'''
				#Debug option, limit processing while chaning documen structure
				i+=1
				if i == 1000:
					break
				

				'''



'''Added this bit since some nodes threw errors when looking for expected 
attributes we're going to get an error log that we can refernce'''
def get_attribute(tag, keyname, errlog):
	with open(errlog, 'w') as fp:
		errout = csv.writer(fp)
	try:
		return tag.attrib[keyname]
	
	except:
		print tag.attrib
		errout.writerow(tag.attrib)


def find_strange_ways(filename):
	#deprecated/unused
	for event, elem in ET.iterparse(filename, events=('start',)):
		way_tags = {}
		if elem.tag == 'way':
			for tag in elem.iter('tag'):
				if  tag.attrib['k'] == 'addr:street':
					build_street_types(way_endings, tag.attrib['v'])

			
	
def dbStats():
	'''
	This section fulfils the requiremetns for Overview of the data in the grading rubric

	'''
	stats = {
				'totalsize': db.command('collStats', DBNAME)['storageSize'],
				'users' : len(cur.distinct('userid')),
				'nodes' : cur.count( {'type':'node'} ),
				'ways' : cur.count( {'type':'way'} ),
				'publix_count' : cur.count({'name':re.compile('.*ublix.*')}),
				'high_school_count' : cur.count({'name': re.compile('.*High School.*')}),
				'elem_school_count' : cur.count({'name': re.compile('.*Elementary School.*')})

	}
	
	print stats


	




def profileData(filename):
	'''
		This will spit out some information about the data we're preocessing
		we have the specs but I'd like to see how many of each tag we're 
		dealing with
	'''
	profile_file_name = 'profile_'+ filename
	with open(profile_file_name, 'w') as fp:
		tags = {}
		way_tags = {}
		tag_types={}

		csv_writer = csv.writer(fp)

		for event, elem in ET.iterparse(filename,  events=('start',)):
			try:
				tags[elem.tag] += 1

			except:
				tags[elem.tag] = 1
				print elem.tag

			for tag in elem.iter('tag'):
				try:
					#print tag.attrib
					way_tags[tag.attrib['k']] += 1
					#print way_tags

				except:
					print tag.attrib
					way_tags[tag.attrib['k']] = 1
			#break
			elem.clear()
			break

		#Write header for tag dump section
		fp.write(str(len(tags))+" diff tags in document:")
		#write out all of the tags we've collected along with their counts
		for k, v in tags.items():
			csv_writer.writerow([k,v])

		#Write out all of the differnt type of tags
		fp.write('\ntag subtypes\n')
		for k, v in way_tags.items():
			csv_writer.writerow([k,v])

		print len(tags)
		print tags
		print way_tags


	

def audit_data():
	'''
	Checking for missing values gives us a check on multile levesl for the audit
	'''
	query_missing_gps =  {'$or' :[{'long' :{'$exists': False}}, \
							{'lat':{'$exists': False}} ] }
	query_missing_name = {'name' : {'$exists': False}}
	query_missing_username =  {'username' : {'$exists': False}}
	query_missing_userid =  { 'userid' : {'$exists': False}}
	

	query_builder = {'type': 'node'}
	query_builder.update( query_missing_username)
	query_builder.update( query_missing_userid)
		
	for record in cur.find(query_builder):
		print record



def main():

	#Inital parsing for adding new feilds to DB
	extract_and_write_nodes_to_db(OSMFILE)
	

	#test consistency by finding mismatched long lat pairs
	audit_data()

	#cleaning task to check streetnames
	check_street_endings()
	
	#Dump stats for db per specs
	dbStats()
	print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__': main()