import csv
from bs4 import BeautifulSoup
from pymongo import MongoClient
import xml.etree.cElementTree as ET
import re
import pprint
import json
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
split_endings_re = re.compile(r'\b\S+\.?$')


street_mappings = {'St': 'Street',
		            'St.': 'Street',
		            'Ave':'Avenue',
		            'Rd.': 'Road',
		            'Blvd': 'Boulevard',
		            'Ct':' Court'}


name_fixes = {  'H.S.': 'High School',
                'HS': 'High School'}
records_with_errors = set()


def xml_to_json(filename):
	#!!!
	
	with open(filename, 'r') as osmfile:
		#Parse through the file interatively
		#Debug option, limit processing
		element= { 'type': None,
					'attribs': {},
					'child': [{}] #[{'type': None,	'attribs':{} }]
					
					}
		attrib_data = []	
		i=0
		for event, elem in ET.iterparse(filename, events=('end',)):
			element['type'] = elem.tag
			#print elem.attrib			
			element['attribs'] = elem.attrib
			if elem.tag =='node' or elem.tag =='way' or elem.tag =='relation':
				print element['type']
				
				#print elem.tag, elem.attrib
				for tag in elem.iter('tag'):
					
					
					pprint.pprint( [{'type': tag.tag, 'attribs': tag.attrib }] )
					
					#print element['child']
					#!!!
					'''
						child.attrib element isn't putting attribs in the right place. need to add it to the dict
					'''

					if element.get('child'):
					 	element['child']+= [{'type': tag.tag, 'attribs': [tag.attrib ]}]
					else:
					 	element.update( {'child':[{'type': tag.tag, 'attribs': [tag.attrib ]}]})
					 
					#attrib_data +=[{tag.tag: tag.attrib}]
					#print element['child']['type'], element['child']['attribs']

					if tag.get('addr:postcode') != None:
						print "found add"
						
			
			#pprint.pprint(element)
			#pprint.pprint(element)
			cur.insert(element)
			element.clear()

			'''
			i+=1
			if i == 1000:
				break
			'''

def fix_key_value_pairs():
	'''
		This function changes the dictionary values  like {k: <key_val>, v: <val_val>}
		to {<key_val>: <val_val}. There is also the issue of the value
	'''
	#First first top level node values
	new_key = ""
	for elem in cur.find({'attribs.k': {'$exists': True}}):
		
		elem['attribs'].update( { elem['attribs']['k'] :elem['attribs']['v']})
		del elem['attribs']['k']
		del elem['attribs']['v']
		print cur.replace_one({'_id': elem['_id']},  elem, False)

	#Next fix child node level
	
	for elem in cur.find({'child.attribs.k': {'$exists': True}}):
		#print "pre"
		#print elem
		for child in elem['child']:
			for attrib in child['attribs']:
				
				new_key.replace(':', '_')

				attrib.update( {attrib['k']:attrib['v']})
				#print attrib['k']
				del attrib['k']
				del attrib['v']
				
		print cur.replace_one({'_id': elem['_id']},  elem, False)
		#print "post"
		#print elem

def find_streets():
	
	for elem in cur.find({'child.attribs.addr:street': {'$exists': True}}):
		for child in elem['child']:
			for attrib in child['attribs']:
				if  attrib.get('addr:street'):
					#print "found street"
					#print attrib['addr:street']
					attrib['addr:street'] =  fix_street_name(attrib['addr:street'])

		print cur.replace_one({'_id': elem['_id']},  elem, False)



def fix_street_name(street):
	'''
		This menthod fixes the street endings that aren't expected and replaces them
		with the dict values contained in street_mappings
		The input it s dict with the ddr tag and the value is the street name
	'''
	#print street

	split_endings_re_search = split_endings_re.search(street).group()
	if split_endings_re_search in street_mappings:
		#replace the incorrect entry identifyed with the dict mapping
		return re.sub(split_endings_re, street_mappings[split_endings_re_search], street) 



def find_postal_code():
	for elem in cur.find({'child.attribs.addr:postcode': {'$exists': True}}):
		for child in elem['child']:
			for attrib in child['attribs']:
				if attrib.get('addr:postcode') :
					
					attrib['addr:postcode'] =  fix_postal_code(attrib['addr:postcode'])
					#print attrib['addr:postcode']

		#print cur.replace_one({'_id': elem['_id']},  elem, False)
		cur.replace_one({'_id': elem['_id']},  elem, False)


def fix_postal_code(zip):
	'''
		Takes Dict from find postal coded with the key of addr:postcode and value of the postal code
	'''
	
	if (len(zip) != 5) and (len(zip) !=10):
		#print 'zip error to fix: '+ zip
		#print len(zip)
		if zip.find(' ') > 0:
			print zip.find(' ')
			return split_endings_re.search(zip).group()
	
	#Check if zip code isn't in FL by checking digiti
	elif int(zip[0]) != 3:
		print 'Error: Zip not in FL: '+ zip
		print split_endings_re.search(zip)



def write_out_fix_me():
	data = []
	with open('fixme.json', 'w') as fp:
		csv_writer = csv.writer(fp)
		for elem in cur.find({'child.attribs.fixme' : {'$exists': True}}) :
		 	print type(elem)
		 	
		 	print elem
		 	data+=elem
		 	csv_writer.writerow(elem)
		 	#json.dump(elem, fp)
		 	#csv.writer(out_file, delimieter=", ").writerow(elem)
		#json.dump(data, out_file)
		print len(data)
		#print data
		#json.dump(data, out_file)




def dbStats():
	'''
	This section fulfils the requirements for Overview of the data in the grading rubric

	'''
	stats = {
				'total_db_size': db.command('collStats', DBNAME)['storageSize'],
				'unique_users' : len(cur.distinct('userid')),
				'unique_nodes' : cur.count( {'type':'node'} ),
				'unique_ways' : cur.count( {'type':'way'} ),
				

	}
	
	pprint.pprint( stats)

def intresting_data():
	
	pipeline = [{'$match': {'child.attribs.amenity': 'place_of_worship'}},
				{'$unwind': 'child.attribs.religion'}, \
				#{'$group': {'_id':'$child.attribs.religion', 'count': {'$sum':1}}}, \
				{'$sort': {'count':-1}}, \
				
				{'$project':{ 'religion':'$child.attribs.religion', 'count':1}}, \
				{'$limit': 10} ]

	religions = cur.aggregate(pipeline)
	for reli in religions:
		print reli

	stats = {
				'num_publix' : cur.count({'child.attribs.name':re.compile('.*ublix.*')}),
				'num_high_school' : cur.count({'child.attribs.name': re.compile('.*High School.*')}),
				'num_elem_school' : cur.count({'child.attribs.name': re.compile('.*Elementary School.*')}),
				'num_of_churches' : cur.count({'child.attribs.amenity': 'place_of_worship'},
			)
	}
	pprint.pprint(stats)
	 



def topContributors():
	pipeline = [{'$match': {'attribs.user': {'$exists': True}}},
				{'$group': {'_id':'$attribs.user', 'count': {'$sum':1}}}, \
				{'$sort': {'count':-1}}, \
				{'$project':{ 'uid': 1, 'user':1, 'count':1}}, \
				{'$limit': 10} ]

	print 'Top Contributors to Map:'					
	for i, user in enumerate(cur.aggregate(pipeline), start=1):
		print str(i)+'. '+user['_id']+' made '+str(user['count'])+' contributions'

	

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
				#print elem.tag

			for tag in elem.iter('tag'):
				try:
					#print tag.attrib
					way_tags[tag.attrib['k']] += 1
					#print way_tags

				except:
					#print tag.attrib
					way_tags[tag.attrib['k']] = 1
			#break
			elem.clear()


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
		#print tags
		#print way_tags
		





def main():

	#Inital parsing for adding new feilds to DB
	#extract_and_write_nodes_to_db(OSMFILE)
	
	#Profile Data
	profileData(OSMFILE)
	#1) First pull xml elements to database:
	#xml_to_json(OSMFILE)

	#2) Convert k, v dicts to actual values
	#fix_key_value_pairs()

	#3) Find addresses
	#find_streets()


	#4) Find Postal Code
	#find_postal_code()

	#5) Fix Me Data
	#write_out_fix_me()

	#6) Print out collection stats for data set
	#dbStats()


	#7( Print out top contributors
	#topContributors()
	
	

	print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__': main()

