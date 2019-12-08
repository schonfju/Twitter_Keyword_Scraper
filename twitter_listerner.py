###########################################
##
## Twitter Gather: Twitter Listener 1.0
## Author: Justin Schonfeld, schonfju@gmail.com
## Last Modified: 31-10-2016
##
## Gathers data from the Twitter Public Streaming API
## Input:
## * Expects to find a 'listener_queries.txt' text file containing tab delimted pairs of terms and subject headings.
## Output:
## * error_log.txt
## Known issues:
## * The exception handler is generalized rather than specialized.  There is only one response to any flag thrown up by twitter - Wait 15 minutes and try again.
##

from __future__ import print_function
import tweepy # Wrapper Library for interacting with Twitter API 'tweepy.readthedocs.io/en/latest'
from tweepy.streaming import Stream # Redundant
from tweepy.auth import OAuthHandler # Redundant
from tweepy.streaming import StreamListener # Redundant
import time 
import datetime
import json
import pickle
import sys
import sqlite3 # Wrapper Library for interating with SQLite database - Included in Python default release


# Location and name of the database
DATABASE_PATH = "./twitter_listener_data.sqlite"

# Key's are associated with a Twitter Account 
ckey = 
csecret = 
atoken = 
asecret = 

# The SQLite database contains a number of tables.  This class 
# handles the creation of said tables and the insertion of data into them. 
class TTDO:
	"""Tweet Table Data Object"""

	# Member Variables ##############
	fields=dict()
	table_name = ""

	# Member Functions ##############

	# Constructor (expects a variable which describes the Twitter Table Data Object)
	def __init__(self, flist, tname):
		self.fields={}
		self.table_name = tname
		for i in range(len(flist)):
			self.fields[flist[i][0]]=dict()
			self.fields[flist[i][0]]["Type"]=flist[i][1]
			self.fields[flist[i][0]]["Value"]=flist[i][2]

	# Sets the value of a variable in the TTDO fields dictionary
	def setValue(self, flabel, fvalue):
		self.fields[flabel]["Value"] = fvalue

	# Returns the SQL string necessary to create the table
	def tableString(self):
		tstr = "CREATE TABLE IF NOT EXISTS "+self.table_name+" ("
		find = list(self.fields.keys())
		nv = len(find)
		for i in range(nv-1):
			tstr += find[i] + " " + self.fields[find[i]]["Type"] + ","			
		tstr += find[nv-1] + " " + self.fields[find[nv - 1]]["Type"] + ")"
		return tstr

	# Returns the SQL string necessary to insert a  record into the table
	def insertString(self):
		istr = "INSERT INTO "+self.table_name + " ("
		find = list(self.fields.keys())
		nv = len(find)

		# Add the column labels to the query
		for i in range(nv-1):
			istr += find[i] + ","
		# Add the last column label
		istr += find[nv-1] + ") VALUES ("

		# Add the values to the query
		for i in range(nv-1):
			if self.fields[find[i]]["Type"]=="INTEGER":
				istr += str(self.fields[find[i]]["Value"])+","
			elif self.fields[find[i]]["Type"]=="TEXT":
				istr += "\""+str(self.fields[find[i]]["Value"])+"\","
			else:
				print("Error! Unhandled Type inserted into database.")
		
		# Add the last value
		if self.fields[find[nv-1]]["Type"]=="INTEGER":
			istr += str(self.fields[find[nv-1]]["Value"])+")"
		elif self.fields[find[nv-1]]["Type"]=="TEXT":
			istr += "\""+str(self.fields[find[nv-1]]["Value"])+"\")"
		else:
			print("Error! Unhandled Type inserted into database.")

		# istr += "\"JS_TWITTER_LISTENER\")"

		return istr

# This is a helper function to determine if the search terms are in the tweet (case insensitive)
def sin(los,fstr):
	t = False
	for astr in los:
		for x in astr.split():
			if x.lower() in fstr.lower():
				t = True
			else:
				t = False
				break
		if t == True:
			break
	return t

# This is a helper function to insert a new record into the table
def insert_string(tbl,tid):
	conn = sqlite3.connect(DATABASE_PATH)
	cur = conn.cursor()
	# Check if the tweetid is already in the table, prevents duplication
	cur.execute("SELECT 1 FROM "+tbl.table_name+" WHERE TweetID = " + str(tid))
	ans = cur.fetchone()
	if not ans:
		cur.execute(tbl.insertString())
	conn.commit()
	cur.close()
	conn.close()

#This sorts the tweets into the right files
class listener(StreamListener):
	
	def on_data(self, data):
		try:
			# Build the query dictionary
			qdict=load_qdict()
			for qk in qdict:
				queries = []
				for el in qdict[qk]:
					queries.append(el)

				if sin(queries,data):
					# Convert the data from json into a readable format
					dat = json.loads(data)
					# fn = qk.strip() + "_" + time.strftime("%d_%m_%Y") + ".txt" # Legacy code

					# Format the data for entry into the database
					text = dat['text']
					text = text.replace("\"","\'")
					if dat["user"]["location"] == None:
						location = "None"
					else:
						location = dat["user"]["location"]


					# Fill in the values for the primary table
					td.setValue("CreatedAt",str(dat["created_at"]))
					td.setValue("Message",text)
					if dat["coordinates"] != None:
						td.setValue("XCoordinate",str(dat["coordinates"]["coordinates"][0]))
						td.setValue("YCoordinate",str(dat["coordinates"]["coordinates"][1]))
					else:
						td.setValue("XCoordinate","None")
						td.setValue("YCoordinate","None")
					td.setValue("FavoriteCount",dat["favorite_count"])
					if dat["in_reply_to_screen_name"] != None:
						td.setValue("InReplyScreenName",dat["in_reply_to_screen_name"])
					else:
						td.setValue("InReplyScreenName","None")
					if dat["in_reply_to_user_id"] != None:
						td.setValue("InReplyUserID",dat["in_reply_to_user_id"])
					else:
						td.setValue("InReplyUserID",-1)
					if dat["in_reply_to_status_id"] != None:
						td.setValue("InReplyTweetID",dat["in_reply_to_status_id"])
					else:
						td.setValue("InReplyTweetID",-1)
					if dat["place"] != None:
						td.setValue("Place",dat["place"]["full_name"])
					else:
						td.setValue("Place","None")
					if dat["user"]["name"] != None:
						td.setValue("Name",dat["user"]["name"])
					else:
						td.setValue("Name","None")
					td.setValue("AccountCreated",dat["user"]["created_at"])
					td.setValue("TimeZone",dat["user"]["time_zone"])
					td.setValue("UTCOffset",dat["user"]["utc_offset"])
					td.setValue("UserID",dat["user"]["id"])
					td.setValue("ScreenName",str(dat["user"]["screen_name"]))
					td.setValue("Location",location)
					td.setValue("FollowersCount",dat["user"]["followers_count"])
					td.setValue("FriendsCount",dat["user"]["friends_count"])
					td.setValue("RetweetCount",dat["retweet_count"])
					td.setValue("TweetID",dat["id"])

					# Check if the database is already in the system
					if dat["contributors"] != None:
						for el in dat["contributors"]:
							cd.setValue("TweetID",dat["id"])
							cd.setValue("ContributorID",el["id"])
							insert_string(cd,dat["id"])

					if dat["entities"]["urls"] != None:
						for el in dat["entities"]["urls"]:
							ud.setValue("TweetID",dat["id"])
							if el["url"] != None:
								ud.setValue("URL",el["url"])
							else:
								ud.setValue("URL","")
							insert_string(ud,dat["id"])

					if dat["entities"]["hashtags"] != None:
						for el in dat["entities"]["hashtags"]:
							hd.setValue("TweetID",dat["id"])
							hd.setValue("Hashtag",el["text"])
							insert_string(hd,dat["id"])

					if dat["entities"]["symbols"] != None:
						for el in dat["entities"]["symbols"]:
							sd.setValue("TweetID",dat["id"])
							sd.setValue("Symbol",el["text"])
							insert_string(sd,dat["id"])

					insert_string(td,dat["id"])

		except BaseException as e:
			print('Failed on_data() ', str(e))
			print(dat)
			outf=open("error_log.txt","a")
			outf.write(str(datetime.datetime.now().isoformat())+"\t"+str(status)+"\n")
			outf.close()
			time.sleep(910)
	
	def on_error(self, status):
		print('On Error! '+str(status))
		outf=open("error_log.txt","a")
		outf.write(str(datetime.datetime.now().isoformat())+"\t"+str(status)+"\n")
		outf.close()

		#if we have exceeded our rate limit
		if status == 420 or status == 429:
			#Wait 15 minutes before attempting to reconnect
			print("Rate Limit Exceeded.  Sleeping for 15 minutes.")
			time.sleep(901)
			return False

# Load the set of queries to search for
def load_qdict():
	qdict = dict()

	#Open the file
	with open('listener_queries.txt','r') as inf:
		for line in inf:
			sline = line.split('\t')
			if len(sline) > 2 or len(sline) < 2:
				continue
			if sline[1] not in qdict:
				qdict[sline[1]] = list()
				qdict[sline[1]].append(sline[0])
			else:
				qdict[sline[1]].append(sline[0])

	if not qdict:
		print("Dictionary of queries is empty.  Check \"listener_queries.txt\".")
		exit()

	return qdict

# CODE EXECUTION begins here
if __name__ == '__main__':
	#This gets the tweets from twitter       
	#Set the codes to authenticate the application and user with twitter
	auth = OAuthHandler(ckey, csecret)
	auth.set_access_token(atoken, asecret)

	#Set the cycle count to 0
	cycle_count = 0

	#Read in the queries from the 'listener_queries.txt' file and populate the query dictionary.
	qdict = load_qdict()

	#Generate the full query list
	queries = []
	for qk in qdict:
		for el in qdict[qk]:
			queries.append(el)
	print(queries)

	ftlist = [("CreatedAt","TEXT",None),("Message","TEXT",None),("UserID","INTEGER",None),("ScreenName","TEXT",None),("Location","TEXT",None),
			 ("FollowersCount","INTEGER",None),("FriendsCount","INTEGER",None),("RetweetCount","INTEGER",None),("TweetID","INTEGER",None),
			 ("DataAgent","TEXT","JS Listener"),("XCoordinate","TEXT",None),("YCoordinate","TEXT",None),("FavoriteCount","INTEGER",None),
			 ("InReplyScreenName","TEXT",None),("InReplyUserID","INTEGER",None),("InReplyTweetID","INTEGER",None),("Place","TEXT",None),
			 ("Name","TEXT",None),("AccountCreated","TEXT",None),("TimeZone","TEXT",None),("UTCOffset","TEXT",None)]
	td = TTDO(ftlist,"VTweets")

	ctlist = [("TweetID","INTEGER",None),("ContributorID","INTEGER",None)]
	cd = TTDO(ctlist,"VTContributors")

	utlist = [("TweetID","INTEGER",None),("URL","TEXT","")]
	ud = TTDO(utlist,"VTURLs")

	stlist = [("TweetID","INTEGER",None),("Symbol","TEXT","")]
	sd = TTDO(stlist,"VTSymbols")

	htlist = [("TweetID","INTEGER",None),("Hashtag","TEXT","")]
	hd = TTDO(htlist,"VTHashtags")

	setlist = [("TweetID","INTEGER",None),("Annotation","INTEGER",-1),("Model","TEXT",""),("Date","TEXT","")]
	sed = TTDO(setlist,"VTSentiment")

	# Connect to the database and create the table if it doesn't already exist
	conn = sqlite3.connect(DATABASE_PATH)
	cur = conn.cursor()
	
	cur.execute(td.tableString()) 
	cur.execute(cd.tableString()) # Contributor Table
	cur.execute(ud.tableString()) # URL Table
	cur.execute(sd.tableString()) # Symbol Table
	cur.execute(hd.tableString()) # Hashtag Table
	cur.execute(sed.tableString()) # Sentiment Table

	conn.commit()
	cur.close()
	conn.close()

	#Begin the main program loop
	while (1):
		try:
			print("Beginning Streaming Cycle: " + str(cycle_count))
			print("Tracking Terms:",end="")
			for query in queries:
				print(" "+str(query),end="")
			print("\n",end="")
			twitterStream = Stream(auth, listener())
			twitterStream.filter(track=queries)
		except:
			time.sleep(10)
			outf=open("error_log.txt","a")
			outf.write(str(datetime.datetime.now().isoformat())+"\tRestarting Now\n")
			outf.close()
			print("Ending Streaming Cycle")
			cycle_count = cycle_count + 1
			
			continue