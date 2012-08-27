from urllib2 import Request, HTTPError, urlopen
from base64 import encodestring
from xml.dom.minidom import Document, parseString
from datetime import datetime

LOGIN_URL = 'http://www.strategyard.com/sy/api/v10/login'
LOGOUT_URL = 'http://www.strategyard.com/sy/api/v10/logout'
STRAT_NEW_URL = 'http://www.strategyard.com/sy/api/v10/addNewStrategy'
STRAT_LIST_URL = 'http://www.strategyard.com/sy/api/v10/strategies'
STRAT_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s'
STRAT_BY_NAME_URL = 'http://www.strategyard.com/sy/api/v10/strategy?name=%s'
STRAT_POSITIONS_LIST_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s/openpositions'
STRAT_TRADE_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s/action/trade'
STRAT_PENDING_LIST_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s/pendingorders'
STRAT_PENDING_CANCEL_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s/pendingorder/%s/'

USERNAME = 'delta'
PASSWORD = 'all seeing eye'

def parseValue(value):
    """
    helper function to parse numbers or dates from responses
    currently no support for timezones
    """
    if value.isdigit():
        value = int(value)
    else:
        try:
            value = float(value)
        except ValueError:
            try:
                value = datetime.strptime(value[:-6], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
    return value
                        

def parseDict(data):
    """
    helper function to parse dictionaries from xml responses
    """
    result = dict()
    if data.hasAttribute('url'):
        result['url'] = data.getAttribute('url')
    for child in data.childNodes:
        if child.hasAttribute('Url'):
            value = child.getAttribute('Url')
        else:
            if child.firstChild:
                value = parseValue(child.firstChild.data)
            else:
                value = ''
        result[str(child.tagName)] = value
    return result

def createXml(rootNode, items):
    doc = Document()
    root = doc.createElement(rootNode)
    root.setAttribute('xmlns', 'http://www.strategyard.com')
    for key, value in items:
        child = doc.createElement(key)
        text = doc.createTextNode(value)
        child.appendChild(text)
        root.appendChild(child)
    doc.appendChild(root)
    return doc.toxml(encoding="utf-8")

def parseError(e):
    return Exception(parseString(e).getElementsByTagName('Message')[0].firstChild.data)


class StrategYardAPI:

    def __init__(self):
        self.token = None
        self.client = None
        self.sid = None

    def buildRequest(self, url, method=None, data=None):
        """
        returns a Request object with the authentication headers prepopulated
        """
        req = Request(url)
        req.add_header("strategyard-authen-token", self.token)
        req.add_header("strategyard-client-id", self.client)
        if method:
            req.get_method = lambda: method
        if data:
            req.add_header("Content-type", "application/xml; charset=UTF-8")
            req.add_data(data)
        return req

    def handleRequest(self, req):
        try:
            resp = urlopen(req)
        except HTTPError as e:
            msg = e.read()
            if msg.find("Unauthorized") > -1:
                self.login()
                req.add_header("strategyard-authen-token", self.token)
                req.add_header("strategyard-client-id", self.client)
                resp = urlopen(req)
            else:
                raise parseError(msg)
        return resp

    def setStrategy(self, sid):
        self.sid = sid

    def currentStrategy(self):
        if not self.sid:
            raise Exception("This action requires a strategy")
        return self.sid

    def login(self, username=USERNAME, password=PASSWORD):
        """
        login to StrategyYard and store credentials
        """
        req = Request(LOGIN_URL)
        b64auth = encodestring('%s:%s' % (USERNAME, PASSWORD))[:-1]
        req.add_header("Authorization", "Basic %s" % b64auth)
        try:
            handle = urlopen(req)
            self.token = handle.headers.get('strategyard-authen-token')
            self.client = handle.headers.get('strategyard-client-id')
            return True
        except HTTPError as e:
            raise parseError(e.read())

    def logout(self):
        """
        logout and remove credentials
        """
        req = self.buildRequest(LOGOUT_URL)
        self.client = None
        self.token = None
        try:
            self.handleRequest(req)
            return True
        except HTTPError as e:
            raise parseError(e.read())

    def addStrategy(self, name, amount, description='', market='US'):
        xml = createXml("NewStrategy", (
            ("Name", unicode(name)),
            ("Description", unicode(description)),
            ("Market", unicode(market)),
            ("InitialAmount", unicode(amount)),
        ))
        req = self.buildRequest(STRAT_NEW_URL, method='PUT', data=xml)
        try:
            resp = self.handleRequest(req).read()
            dom = parseString(resp)
            return parseDict(dom.childNodes[0])
        except HTTPError as e:
            raise parseError(e.read())

    def deleteStrategy(self, sid = None):
        sid = sid or self.currentStrategy()
        req = self.buildRequest(STRAT_URL % sid, method="DELETE")
        try:
            self.handleRequest(req)
            return True
        except HTTPError as e:
            raise parseError(e.read())

    def listStrategies(self):
        req = self.buildRequest(STRAT_LIST_URL)
        try:
            resp = self.handleRequest(req).read()
            dom = parseString(resp)
            return [parseDict(strat) for strat in dom.getElementsByTagName('strategy')]
        except HTTPError as e:
            raise parseError(e.read())

    def getStrategy(self, sid, use = False):
        req = self.buildRequest(STRAT_URL % sid)
        try:
            resp = self.handleRequest(req).read()
            dom = parseString(resp)
            ret = parseDict(dom.childNodes[0])
            if use:
                self.sid = sid
            return ret
        except HTTPError as e:
            raise parseError(e.read())

    def getStrategyByName(self, sname, use = False):
        req = self.buildRequest(STRAT_BY_NAME_URL % sname)
        try:
            resp = self.handleRequest(req).read()
            dom = parseString(resp)
            ret = parseDict(dom.childNodes[0])
            if use:
                self.sid = ret['Id']
            return ret
        except HTTPError as e:
            raise parseError(e.read())

    def getOpenPositions(self, sid = None):
        sid = sid or self.currentStrategy()
        req = self.buildRequest(STRAT_POSITIONS_LIST_URL % sid)
        try:
            resp = self.handleRequest(req).read()
            dom = parseString(resp)
            return [parseDict(position) for position in dom.getElementsByTagName('OpenPosition')]
        except HTTPError as e:
            raise parseError(e.read())

    def trade(self, ticker, quantity, order='BUY', sid = None):
        sid = sid or self.currentStrategy()
        xml = createXml("TradeAction", (
            ("Ticker", unicode(ticker)),
            ("OrderType", unicode(order)),
            ("Quantity", unicode(quantity)),
        ))
        req = self.buildRequest(STRAT_TRADE_URL % sid, method='POST', data=xml)
        try:
            self.handleRequest(req)
            return True
        except HTTPError as e:
            import ipdb; ipdb.set_trace()
            raise parseError(e.read())

    def getPendingOrders(self, sid = None):
        sid = sid or self.currentStrategy()
        req = self.buildRequest(STRAT_PENDING_LIST_URL % sid)
        try:
            resp = self.handleRequest(req).read()
            dom = parseString(resp)
            return [parseDict(position) for position in dom.getElementsByTagName('PendingOrder')]
        except HTTPError as e:
            raise parseError(e.read())

    def cancelPendingOrder(self, oid, sid = None):
        sid = sid or self.currentStrategy()
        req = self.buildRequest(STRAT_PENDING_CANCEL_URL % (sid, oid), method="DELETE")
        try:
            self.handleRequest(req)
            return True
        except HTTPError as e:
            raise parseError(e.read())
        
