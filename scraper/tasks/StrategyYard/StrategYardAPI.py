from urllib2 import Request, HTTPError, urlopen
from base64 import encodestring
from xml.dom.minidom import Document, parseString
from datetime import datetime

_LOGIN_URL = 'http://www.strategyard.com/sy/api/v10/login'
_LOGOUT_URL = 'http://www.strategyard.com/sy/api/v10/logout'
_STRAT_NEW_URL = 'http://www.strategyard.com/sy/api/v10/addNewStrategy'
_STRAT_LIST_URL = 'http://www.strategyard.com/sy/api/v10/strategies'
_STRAT_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s'
_STRAT_BY_NAME_URL = 'http://www.strategyard.com/sy/api/v10/strategy?name=%s'
_STRAT_POSITIONS_LIST_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s/openpositions'
_STRAT_TRADE_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s/action/trade'
_STRAT_PENDING_LIST_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s/pendingorders'
_STRAT_PENDING_CANCEL_URL = 'http://www.strategyard.com/sy/api/v10/strategy/%s/pendingorder/%s/'

_USERNAME = 'delta'
_PASSWORD = 'all seeing eye'

def _parseValue(value):
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
                        

def _parseDict(data):
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
                value = _parseValue(child.firstChild.data)
            else:
                value = ''
        result[str(child.tagName)] = value
    return result

def _createXml(rootNode, items):
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

def _parseError(e):
    return Exception(parseString(e).getElementsByTagName('Message')[0].firstChild.data)


class StrategYardAPI:

    def __init__(self):
        self.__token = None
        self.__client = None
        self.__sid = None

    def __buildRequest(self, url, method=None, data=None):
        """
        returns a Request object with the authentication headers prepopulated
        """
        req = Request(url)
        req.add_header("strategyard-authen-token", self.__token)
        req.add_header("strategyard-client-id", self.__client)
        if method:
            req.get_method = lambda: method
        if data:
            req.add_header("Content-type", "application/xml; charset=UTF-8")
            req.add_data(data)
        return req

    def __handleRequest(self, req):
        try:
            resp = urlopen(req)
        except HTTPError as e:
            msg = e.read()
            if msg.find("Unauthorized") > -1:
                self.login()
                req.add_header("strategyard-authen-token", self.__token)
                req.add_header("strategyard-client-id", self.__client)
                resp = urlopen(req)
            else:
                raise _parseError(msg)
        return resp

    def __currentStrategy(self):
        if not self.__sid:
            raise Exception("This action requires a strategy")
        return self.__sid

    def setStrategy(self, sid):
        self.__sid = sid

    def login(self, username=_USERNAME, password=_PASSWORD):
        """
        login to StrategyYard and store credentials
        """
        req = Request(_LOGIN_URL)
        b64auth = encodestring('%s:%s' % (username, password))[:-1]
        req.add_header("Authorization", "Basic %s" % b64auth)
        try:
            handle = urlopen(req)
            self.__token = handle.headers.get('strategyard-authen-token')
            self.__client = handle.headers.get('strategyard-client-id')
            return True
        except HTTPError as e:
            raise _parseError(e.read())

    def logout(self):
        """
        logout and remove credentials
        """
        req = self.__buildRequest(_LOGOUT_URL)
        self.__client = None
        self.__token = None
        try:
            self.__handleRequest(req)
            return True
        except HTTPError as e:
            raise _parseError(e.read())

    def addStrategy(self, name, amount, description='', market='US'):
        xml = _createXml("NewStrategy", (
            ("Name", unicode(name)),
            ("Description", unicode(description)),
            ("Market", unicode(market)),
            ("InitialAmount", unicode(amount)),
        ))
        req = self.__buildRequest(_STRAT_NEW_URL, method='PUT', data=xml)
        try:
            resp = self.__handleRequest(req).read()
            dom = parseString(resp)
            return _parseDict(dom.childNodes[0])
        except HTTPError as e:
            raise _parseError(e.read())

    def deleteStrategy(self, sid = None):
        sid = sid or self.__currentStrategy()
        req = self.__buildRequest(_STRAT_URL % sid, method="DELETE")
        try:
            self.__handleRequest(req)
            return True
        except HTTPError as e:
            raise _parseError(e.read())

    def listStrategies(self):
        req = self.__buildRequest(_STRAT_LIST_URL)
        try:
            resp = self.__handleRequest(req).read()
            dom = parseString(resp)
            return [_parseDict(strat) for strat in dom.getElementsByTagName('strategy')]
        except HTTPError as e:
            raise _parseError(e.read())

    def getStrategy(self, sid, use = False):
        req = self.__buildRequest(_STRAT_URL % sid)
        try:
            resp = self.__handleRequest(req).read()
            dom = parseString(resp)
            ret = _parseDict(dom.childNodes[0])
            if use:
                self.sid = sid
            return ret
        except HTTPError as e:
            raise _parseError(e.read())

    def getStrategyByName(self, sname, use = False):
        req = self.__buildRequest(_STRAT_BY_NAME_URL % sname)
        try:
            resp = self.__handleRequest(req).read()
            dom = parseString(resp)
            ret = _parseDict(dom.childNodes[0])
            if use:
                self.sid = ret['Id']
            return ret
        except HTTPError as e:
            raise _parseError(e.read())

    def listOpenPositions(self, sid = None):
        sid = sid or self.__currentStrategy()
        req = self.__buildRequest(_STRAT_POSITIONS_LIST_URL % sid)
        try:
            resp = self.__handleRequest(req).read()
            dom = parseString(resp)
            return [_parseDict(position) for position in dom.getElementsByTagName('OpenPosition')]
        except HTTPError as e:
            raise _parseError(e.read())

    def trade(self, ticker, quantity, order='BUY', sid = None):
        sid = sid or self.__currentStrategy()
        xml = _createXml("TradeAction", (
            ("Ticker", unicode(ticker)),
            ("OrderType", unicode(order)),
            ("Quantity", unicode(quantity)),
        ))
        req = self.__buildRequest(_STRAT_TRADE_URL % sid, method='POST', data=xml)
        try:
            self.__handleRequest(req)
            return True
        except HTTPError as e:
            raise _parseError(e.read())

    def listPendingOrders(self, sid = None):
        sid = sid or self.__currentStrategy()
        req = self.__buildRequest(_STRAT_PENDING_LIST_URL % sid)
        try:
            resp = self.__handleRequest(req).read()
            dom = parseString(resp)
            return [_parseDict(position) for position in dom.getElementsByTagName('PendingOrder')]
        except HTTPError as e:
            raise _parseError(e.read())

    def cancelPendingOrder(self, oid, sid = None):
        sid = sid or self.__currentStrategy()
        req = self.__buildRequest(_STRAT_PENDING_CANCEL_URL % (sid, oid), method="DELETE")
        try:
            self.__handleRequest(req)
            return True
        except HTTPError as e:
            raise _parseError(e.read())
        
