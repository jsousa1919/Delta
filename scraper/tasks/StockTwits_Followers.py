import urllib2
from BeautifulSoup import BeautifulSoup
import re
import string
import datetime

def get_page(url):
	header = {"User-Agent": "Mozilla/5.0 (X11; U; Linux x86_64; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.205 Safari/534.16"}
	request = urllib2.Request(url, headers=header)
	try:
		response = urllib2.urlopen(request)
		htmlSource = response.read()
		response.close()
	except urllib2.HTTPError, error:
		print "error: ", error.read()
		htmlSource = None
	return htmlSource
	
#get a user's following twitter/timszlaga
company = "zip"
url = "http://www.stockhouse.com/FinancialTools/sn_newsreleases.asp?symbol="+company+"&asp=1&amp;aord=398576286"
#url = "http://seekingalpha.com/symbol/" + company
htmlSource = get_page(url)
soup = BeautifulSoup(htmlSource)
 
html_out = open('C:\\Users\\SOG\\Desktop\\QFSEED\\delta-aggregate\\twitter-scraper\\tasks\\User-Followings', 'a')

#seeking alpha


#get StockHouse shit
links = soup.findAll("a", href = re.compile("^sn_newsreleases"))
dates = soup.findAll("span", attrs={"class": "ft_stat"})
x = 0 #keep index of date in align with it's link
newLinks = []
ticker_pattern_Nas = re.compile("\Nasdaq: ([A-Z]+(?:\.[A-Z]+)?)")
ticker_pattern_NYSE = re.compile("\NYSE: ([A-Z]+(?:\.[A-Z]+)?)")
ticker_pattern_Nas2 = re.compile("\Nasdaq:([A-Z]+(?:\.[A-Z]+)?)") #patterns for no space after colon
ticker_pattern_NYSE2 = re.compile("\NYSE:([A-Z]+(?:\.[A-Z]+)?)")  ###################################
refs = []
#for link in links:
if links[1].text != "News Releases":
	end = 0 #used to determine when the end of the article is reached
	url = links[18]["href"] 
	headline = links[18].text 
	date = dates[17].text
	article = []
	#if date is after the most recent date in db, add new article
	#get article
	url = "http://www.stockhouse.com/FinancialTools/" + url
	htmlNP = get_page(url)
	soupNP = BeautifulSoup(htmlNP)
	frame = soupNP.find("iframe", id="ctl00_cphMainContent_frmTool") #frame that contains article
	if frame is not None:
		frameLink = "http://www.stockhouse.com"+frame["src"][2:]		 ############################
		frameLinkHTML = get_page(frameLink)
		frameSoup = BeautifulSoup(frameLinkHTML)
		artPiece = frameSoup.findAll("p") #all paragraphs in article
		for piece in artPiece:
			if piece.text[:6] != "About " and end == 0:
				article.append(piece.text)
				if ticker_pattern_Nas.findall(piece.text):
					refs.append(ticker_pattern_Nas.findall(piece.text))
				if ticker_pattern_NYSE.findall(piece.text):
					refs.append(ticker_pattern_NYSE.findall(piece.text))
				if ticker_pattern_Nas2.findall(piece.text):
					refs.append(ticker_pattern_Nas2.findall(piece.text))
				if ticker_pattern_NYSE2.findall(piece.text):
					refs.append(ticker_pattern_NYSE2.findall(piece.text))
			else:
				end = 1
		newLinks.append([frameLink, headline, date, article])
	else:
		print "error getting frame link"
	x = x + 1

date = date[:date.index("-")-1]
try: 
	date = datetime.datetime.strptime(date, "%m/%d/%Y %I:%M %p")
except ValueError:
	date = datetime.datetime.strptime(date, "%m/%d/%Y %I:%M:%S %p")

print date
print refs
print datetime.datetime.now() - date

"""StockTwits
#get "trending now" from StockTwits
for sym in soup.find(attrs={"class": "scrollableArea"}).findAll("p"):
	if sym.text.startswith("$"):
		print sym.text[1:]
"""
"""
#get username
username = soup.find(attrs={"class": "username"})
if username is None:
    print "Error getting username." + "\n"
else:
    #get follwers
    followers = soup.find(attrs={"class": "follow-number"})
    if followers is not None:
		#followers = long(re.sub("\D","", followers.text))
    elif followers is None:
		followers = long(re.sub("\D","", soup.find(id="traderStats").find(href=re.compile("followers$")).find(attrs={"class": "black"}).text))
	
    html_out.write(followers.text) #+ ', ' + str(followers) + '\n')
"""
#####
"""
tweetData = [] #hold user, tweet, and time
content = soup.findAll(attrs={"class": "content"}) #hold all containers with tweet data
for message in content:
	username = message.find(attrs={"class": "username"}) #get username
	tweet = message.find(attrs={"class": "body"}) 		 #get message
	date = message.find(attrs={"class": "msgDate"})		 #get message date
	#only record new tweets
	#if date != db.lastTweet:
	tweetData.append([username.text, tweet.text, date.text])	
html_out.write(username.text + tweet.text+date.text)
"""

"""
#get all usernames that posted
usernames = soup.findAll(attrs={"class": "username"})
userInfo = [] #list of all user infos
for page in usernames:
	#if (datetime.datetime(now) - db.query(last update of page.text).date > datetime.timeDelta(day = 1)) || db.query(page.text) ==None:
	pageURL = "http://stocktwits.com" + page.get("href")
	print pageURL
	header = {"User-Agent": "Mozilla/5.0 (X11; U; Linux x86_64; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.205 Safari/534.16"}
	request = urllib2.Request(pageURL, headers=header)
	try:
		response = urllib2.urlopen(request)
		htmlSource = response.read()
		response.close()
	except urllib2.HTTPError, error:
		print "error: ", error.read()
		htmlSource = " "
	soup = BeautifulSoup(htmlSource) #html soup of user's page
	
	#get user's following
	followers = soup.find(attrs={"class": "follow-number"})
	if followers is not None:
		followers = long(re.sub("\D","", followers.text))
	elif followers is None:
		followers = long(re.sub("\D","", soup.find(id="traderStats").find(href=re.compile("followers$")).find(attrs={"class": "black"}).text))
	else:
		print "Error getting followers."+"\n"
		
	userInfo.append(page.text + ', ' + str(followers))
for UI in userInfo:
	html_out.write(UI + '\n')
"""

html_out.close()