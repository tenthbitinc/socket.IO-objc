//
//  SocketIO.m
//  v.01
//
//  based on 
//  socketio-cocoa https://github.com/fpotter/socketio-cocoa
//  by Fred Potter <fpotter@pieceable.com>
//
//  using
//  https://github.com/square/SocketRocket
//  http://regexkit.sourceforge.net/RegexKitLite/
//  https://github.com/stig/json-framework/
//  http://allseeing-i.com/ASIHTTPRequest/
//
//  reusing some parts of
//  /socket.io/socket.io.js
//
//  Created by Philipp Kyeck http://beta_interactive.de
//

#import "SocketIO.h"

#import "ASIHTTPRequest.h"
#import "SocketRocket/SRWebSocket.h"
#import "RegexKitLite.h"
#import "SBJSON.h"
#import "NSObject+SBJSON.h"
#import "NSString+SBJSON.h"

#define DEBUG_LOGS 0
#define HANDSHAKE_URL @"http://%@:%d/socket.io/1/?t=%d%@"
#define HANDSHAKE_URL_SECURE @"https://%@:%d/socket.io/1/?t=%d%@"
#define SOCKET_URL @"ws://%@:%d/socket.io/1/websocket/%@"
#define SOCKET_URL_SECURE @"wss://%@:%d/socket.io/1/websocket/%@"


# pragma mark -
# pragma mark SocketIO's private interface

@interface SocketIO (FP_Private) <SRWebSocketDelegate>

- (void) log:(NSString *)message;

- (void) setTimeout;
- (void) onTimeout;

- (void) onConnect:(SocketIOPacket *)packet;
- (void) onDisconnect;

- (void) sendDisconnect;
- (void) sendHearbeat;
- (void) send:(SocketIOPacket *)packet;

- (NSString *) addAcknowledge:(SocketIOCallback)function;
- (void) removeAcknowledgeForKey:(NSString *)key;

- (void) webSocketDispose_;

@end


# pragma mark -
# pragma mark SocketIO implementation

@implementation SocketIO

@synthesize isConnected = _isConnected, isConnecting = _isConnecting;
@synthesize currentEndpoint=_currentEndpoint;
@synthesize delegate=_delegate;

- (id) initWithDelegate:(id<SocketIODelegate>)delegate
{
    self = [super init];
    if (self)
    {
        _delegate = delegate;
        
        _queue = [[NSMutableArray alloc] init];
        
        _ackCount = 0;
        _acks = [[NSMutableDictionary alloc] init];
    }
    return self;
}

- (void) connectToHost:(NSString *)host onPort:(NSInteger)port secure:(BOOL)secure
{
    [self connectToHost:host onPort:port secure:secure withParams:nil withNamespace:@""];
}

- (void) connectToHost:(NSString *)host onPort:(NSInteger)port secure:(BOOL)secure withParams:(NSDictionary *)params
{
    [self connectToHost:host onPort:port secure:secure withParams:params withNamespace:@""];
}

- (void) connectToHost:(NSString *)host onPort:(NSInteger)port secure:(BOOL)secure withParams:(NSDictionary *)params withNamespace:(NSString *)endpoint
{
    [self connectToHost:host onPort:port secure:secure withParams:params withNamespaces:[NSArray arrayWithObject:endpoint]];
}

- (void) connectToHost:(NSString *)host onPort:(NSInteger)port secure:(BOOL)secure withParams:(NSDictionary *)params withNamespaces:(NSArray *)endpoints;
{
    if (!_isConnected && !_isConnecting) 
    {
        _isConnecting = YES;
        
        _host = host;
        _port = port;
        _secure = secure;
        _endpoints = [endpoints copy];
        _connectedEndpoints = [NSMutableArray arrayWithCapacity:_endpoints.count];
        _currentEndpoint = @"";
        
        // create a query parameters string
        NSMutableString *query = [[NSMutableString alloc] initWithString:@""];
        [params enumerateKeysAndObjectsUsingBlock: ^(id key, id value, BOOL *stop) {
            [query appendFormat:@"&%@=%@",key,value];
        }];
        
        // do handshake via HTTP request
        NSString *s = nil;
        if(_secure) {
            s = [NSString stringWithFormat:HANDSHAKE_URL_SECURE, _host, _port, rand(), query];
        }else{
            s = [NSString stringWithFormat:HANDSHAKE_URL, _host, _port, rand(), query];
        }
        [self log:[NSString stringWithFormat:@"Connecting to socket with URL: %@",s]];
        NSURL *url = [NSURL URLWithString:s];
        
        _handshakeRequest.delegate = nil;
        _handshakeRequest = [ASIHTTPRequest requestWithURL:url];
        [_handshakeRequest setDelegate:self];
        [_handshakeRequest startAsynchronous];
    }
}

- (void) disconnect
{
    [self sendDisconnect];
}

- (void) sendMessage:(NSString *)data
{
    [self sendMessage:data withAcknowledge:nil];
}

- (void) sendMessage:(NSString *)data withAcknowledge:(SocketIOCallback)function
{
    SocketIOPacket *packet = [[SocketIOPacket alloc] initWithType:@"message"];
    packet.data = data;
    packet.pId = [self addAcknowledge:function];
    [self send:packet];
}

- (void) sendJSON:(NSDictionary *)data
{
    [self sendJSON:data withAcknowledge:nil];
}

- (void) sendJSON:(NSDictionary *)data withAcknowledge:(SocketIOCallback)function
{
    SocketIOPacket *packet = [[SocketIOPacket alloc] initWithType:@"json"];
    packet.data = [data JSONRepresentation];
    packet.pId = [self addAcknowledge:function];
    [self send:packet];
}

- (void) sendEvent:(NSString *)eventName withData:(NSDictionary *)data
{
    [self sendEvent:eventName withData:data andAcknowledge:nil];
}

- (void) sendEvent:(NSString *)eventName withData:(NSDictionary *)data andAcknowledge:(SocketIOCallback)function
{
    NSMutableDictionary *dict = [NSMutableDictionary dictionaryWithObject:eventName forKey:@"name"];
    if (data != nil) // do not require arguments
        [dict setObject:data forKey:@"args"];
    
    SocketIOPacket *packet = [[SocketIOPacket alloc] initWithType:@"event"];
    packet.data = [dict JSONRepresentation];
    packet.pId = [self addAcknowledge:function];
    if (function) 
    {
        packet.ack = @"data";
    }
    [self send:packet];
}

- (void)sendAcknowledgement:(NSString *)pId withArgs:(NSArray *)data {
    SocketIOPacket *packet = [[SocketIOPacket alloc] initWithType:@"ack"];
    packet.data = [data JSONRepresentation];
    packet.pId = pId;
    packet.ack = @"data";
    
    [self send:packet];
}

# pragma mark -
# pragma mark private methods

- (void) openSocket
{
    NSString * urlString = nil;
    if(_secure) {
        urlString = [NSString stringWithFormat:SOCKET_URL_SECURE, _host, _port, _sid];
    }else{
        urlString = [NSString stringWithFormat:SOCKET_URL, _host, _port, _sid];
    }
    NSURL * url = [NSURL URLWithString:urlString];
    NSURLRequest * urlRequest = [NSURLRequest requestWithURL:url];
    
    [self webSocketDispose_];
    
    _webSocket = [[SRWebSocket alloc] initWithURLRequest:urlRequest];
    _webSocket.delegate = self;
    [_webSocket open];
    [self log:[NSString stringWithFormat:@"Opening %@", urlString]];
}

- (void) sendDisconnect
{
    SocketIOPacket *packet = [[SocketIOPacket alloc] initWithType:@"disconnect"];
    [self send:packet];
}

- (void) sendConnect
{
    SocketIOPacket *packet = [[SocketIOPacket alloc] initWithType:@"connect"];
    [self send:packet];
}

- (void) sendConnectToEndpoint:(NSString*)endpoint
{
    _currentEndpoint = endpoint;
    SocketIOPacket *packet = [[SocketIOPacket alloc] initWithType:@"connect"];
    [self send:packet];
}

- (void) sendHeartbeat
{
    SocketIOPacket *packet = [[SocketIOPacket alloc] initWithType:@"heartbeat"];
    [self send:packet];
}

- (void) send:(SocketIOPacket *)packet
{   
    [self log:@"send()"];
    NSNumber *type = [packet typeAsNumber];
    NSMutableArray *encoded = [NSMutableArray arrayWithObject:type];
    
    NSString *pId = packet.pId != nil ? packet.pId : @"";
    if ([packet.ack isEqualToString:@"data"])
    {
        pId = [pId stringByAppendingString:@"+"];
    }
    
    // Do not write pid for acknowledgements
    if ([type intValue] != 6) {
        [encoded addObject:pId];
    }
    
    // Add the end point for the namespace to be used, as long as it is not
    // an ACK, heartbeat, or disconnect packet
    if ([type intValue] != 6 && [type intValue] != 2 && [type intValue] != 0) {
        [encoded addObject:_currentEndpoint];
    } else {
        [encoded addObject:@""];
    }
    
    if (packet.data != nil)
    {
        NSString *ackpId = @"";
        // This is an acknowledgement packet, so, prepend the ack pid to the data
        if ([type intValue] == 6) {
            ackpId = [NSString stringWithFormat:@":%@%@", packet.pId, @"+"];
        }
        
        [encoded addObject:[NSString stringWithFormat:@"%@%@", ackpId, packet.data]];
    }
    
    NSString *req = [encoded componentsJoinedByString:@":"];
    if (!_isConnected) 
    {
        [self log:[NSString stringWithFormat:@"queue >>> %@", req]];
        [_queue addObject:packet];
    } 
    else 
    {
        [self log:[NSString stringWithFormat:@"send() >>> %@", req]];
        [_webSocket send:req];
        
        if ([_delegate respondsToSelector:@selector(socketIO:didSendMessage:)])
        {
            [_delegate socketIO:self didSendMessage:packet];
        }
    }
}



- (void) onData:(NSString *)data 
{
    [self log:[NSString stringWithFormat:@"onData %@", data]];
    
    // data arrived -> reset timeout
    [self setTimeout];
    
    // check if data is valid (from socket.io.js)
    NSString *regex = @"^([^:]+):([0-9]+)?(\\+)?:([^:]+)?:?(.*)?$";
    NSString *regexPieces = @"^([0-9]+)(\\+)?(.*)";
    NSArray *test = [data arrayOfCaptureComponentsMatchedByRegex:regex];
    
    // valid data-string arrived
    if ([test count] > 0) 
    {
        NSArray *result = [test objectAtIndex:0];
        
        int idx = [[result objectAtIndex:1] intValue];
        SocketIOPacket *packet = [[SocketIOPacket alloc] initWithTypeIndex:idx];
        
        packet.pId = [result objectAtIndex:2];
        
        packet.ack = [result objectAtIndex:3];
        packet.endpoint = [result objectAtIndex:4];        
        packet.data = [result objectAtIndex:5];
        
        //
        switch (idx) 
        {
            case 0: {
                [self log:@"disconnect"];
                [self onDisconnect];
                break;
            }    
            case 1: {
                [self log:@"connect"];
                // from socket.io.js ... not sure when data will contain sth?! 
                // packet.qs = data || '';
                [self onConnect:packet];
                break;
            }    
            case 2: {
                [self log:@"heartbeat"];
                [self sendHeartbeat];
                break;
            }
            case 3: {
                [self log:@"message"];
                if (packet.data && ![packet.data isEqualToString:@""])
                {
                    if ([_delegate respondsToSelector:@selector(socketIO:didReceiveMessage:)]) 
                    {
                        [_delegate socketIO:self didReceiveMessage:packet];
                    }
                }
                break;
            }    
            case 4: {
                [self log:@"json"];
                if (packet.data && ![packet.data isEqualToString:@""])
                {
                    if ([_delegate respondsToSelector:@selector(socketIO:didReceiveJSON:)]) 
                    {
                        [_delegate socketIO:self didReceiveJSON:packet];
                    }
                }
                break;
            }
            case 5: {
                [self log:@"event"];
                if (packet.data && ![packet.data isEqualToString:@""])
                { 
                    NSDictionary *json = [packet dataAsJSON];
                    packet.name = [json objectForKey:@"name"];
                    packet.args = [json objectForKey:@"args"];
                    if ([_delegate respondsToSelector:@selector(socketIO:didReceiveEvent:)]) 
                    {
                        [_delegate socketIO:self didReceiveEvent:packet];
                    }
                }
                break;
            }
            case 6: {
                [self log:@"ack"];
                NSArray *pieces = [packet.data arrayOfCaptureComponentsMatchedByRegex:regexPieces];
                
                if ([pieces count] > 0) 
                {
                    NSArray *piece = [pieces objectAtIndex:0];
                    int ackId = [[piece objectAtIndex:1] intValue];
                    [self log:[NSString stringWithFormat:@"ack id found: %d", ackId]];
                    
                    NSString *argsStr = [piece objectAtIndex:3];
                    id argsData = nil;
                    if (argsStr && ![argsStr isEqualToString:@""])
                    {
                        argsData = [argsStr JSONValue];
                        if ([argsData count] > 0)
                        {
                            argsData = [argsData objectAtIndex:0];
                        }
                    }
                    
                    // get selector for ackId
                    NSString *key = [NSString stringWithFormat:@"%d", ackId];
                    SocketIOCallback callbackFunction = [_acks objectForKey:key];
                    if (callbackFunction != nil)
                    {
                        callbackFunction(argsData);
                        [self removeAcknowledgeForKey:key];
                    }
                }
                
                break;
            } 
            case 7: {
                [self log:@"error"];
                break;
            }
            case 8: {
                [self log:@"noop"];
                break;
            }
            default: {
                [self log:@"command not found or not yet supported"];
                break;
            }
        }
    }
    else
    {
        [self log:@"ERROR: data that has arrived wasn't valid"];
    }
}


- (void) doQueue 
{
    [self log:[NSString stringWithFormat:@"doQueue() >> %d", [_queue count]]];
    
    // TODO send all packets at once ... not as seperate packets
    while ([_queue count] > 0) 
    {
        SocketIOPacket *packet = [_queue objectAtIndex:0];
        [self send:packet];
        [_queue removeObject:packet];
    }
}

- (void) onConnect:(SocketIOPacket *)packet
{
    [self log:@"onConnect()"];
    
    _isConnected = YES;
    
    // Send the connected packet so the server knows what it's dealing with.
    // Only required when endpoint/namespace is present
    if ([_endpoints count] > 0) {
        // Make sure the packet we received has an endpoint, otherwise send it again
        if([packet.endpoint length]) {
            if([_connectedEndpoints containsObject:packet.endpoint]) {
                NSLog(@"already connected to: %@",packet.endpoint);
            }else{
                [_connectedEndpoints addObject:packet.endpoint];
            }
        }
        
        if ([_connectedEndpoints count] < [_endpoints count]) {
            NSString *nextEndpoint = nil;
            for(NSString *endpoint in _endpoints) {
                if([_connectedEndpoints containsObject:endpoint]==NO) {
                    nextEndpoint = endpoint;
                    break;
                }
            }
            [self log:[NSString stringWithFormat:@"onConnect() >> will connect to next endpoint: %@",nextEndpoint]];
            [self sendConnectToEndpoint:nextEndpoint];
            return;
        }
    }
    
    _isConnecting = NO;
    
    if ([_delegate respondsToSelector:@selector(socketIODidConnect:)]) 
    {
        [_delegate socketIODidConnect:self];
    }
    
    // send any queued packets
    [self doQueue];
    
    [self setTimeout];
}

- (void) onDisconnect 
{
    [self log:@"onDisconnect()"];
    
    _isConnected = NO;
    _isConnecting = NO;
    _sid = nil;
    
    [_queue removeAllObjects];
    
    // Kill the heartbeat timer
    if (_timeout != nil) {
        [_timeout invalidate];
        _timeout = nil;
    }
    
    // Disconnect the websocket, just in case
    if (_webSocket != nil && _webSocket.readyState != SR_CLOSED ) {
        [self webSocketDispose_];
    }
    
    if ([_delegate respondsToSelector:@selector(socketIODidDisconnect:)]) 
    {
        [_delegate socketIODidDisconnect:self];
    }
}

# pragma mark -
# pragma mark Acknowledge methods

- (NSString *) addAcknowledge:(SocketIOCallback)function
{
    if (function)
    {
        ++_ackCount;
        NSString *ac = [NSString stringWithFormat:@"%d", _ackCount];
        [_acks setObject:[function copy] forKey:ac];
        return ac;
    }
    return nil;
}

- (void) removeAcknowledgeForKey:(NSString *)key
{
    [_acks removeObjectForKey:key];
}

# pragma mark -
# pragma mark Heartbeat methods

- (void) onTimeout 
{
    [self log:@"Timed out waiting for heartbeat."];
    [self onDisconnect];
}

- (void) setTimeout 
{
    [self log:@"setTimeout()"];
    if (_timeout != nil) 
    {   
        [_timeout invalidate];
        _timeout = nil;
    }
    
    _timeout = [NSTimer scheduledTimerWithTimeInterval:_heartbeatTimeout
                                                target:self 
                                              selector:@selector(onTimeout) 
                                              userInfo:nil 
                                               repeats:NO];
}


# pragma mark -
# pragma mark Handshake callbacks

- (void) requestFinished:(ASIHTTPRequest *)request
{
    NSString *responseString = [request responseString];
    [self log:[NSString stringWithFormat:@"requestFinished() %@", responseString]];
    NSArray *data = [responseString componentsSeparatedByString:@":"];
    
    if ([data count]>=4)
    {
        _sid = [data objectAtIndex:0];
        [self log:[NSString stringWithFormat:@"sid: %@", _sid]];
        
        // add small buffer of 7sec (magic xD)
        _heartbeatTimeout = [[data objectAtIndex:1] floatValue] + 7.0;
        [self log:[NSString stringWithFormat:@"heartbeatTimeout: %f", _heartbeatTimeout]];
        
        // index 2 => connection timeout
        
        NSString *t = [data objectAtIndex:3];
        NSArray *transports = [t componentsSeparatedByString:@","];
        [self log:[NSString stringWithFormat:@"transports: %@", transports]];
    
        [self openSocket];
    }else{
        NSLog(@"ERROR: handshake failed with status code %d. Response string: %@", request.responseStatusCode, request.responseString);
        
        _isConnected = NO;
        _isConnecting = NO;
        
        if ([_delegate respondsToSelector:@selector(socketIOHandshakeFailed:)])
        {
            [_delegate socketIOHandshakeFailed:self];
        }
    }
}

- (void) requestFailed:(ASIHTTPRequest *)request
{
    NSError *error = [request error];
    NSLog(@"ERROR: handshake failed ... %@", [error localizedDescription]);
    
    _isConnected = NO;
    _isConnecting = NO;
    
    if ([_delegate respondsToSelector:@selector(socketIOHandshakeFailed:)])
    {
        [_delegate socketIOHandshakeFailed:self];
    }
}

# pragma mark -
# pragma mark Web Socket Delegate Methods

- (void)webSocket:(SRWebSocket *)webSocket didCloseWithCode:(NSInteger)code reason:(NSString *)reason wasClean:(BOOL)wasClean {
    [self log:[NSString stringWithFormat:@"Connection closed."]];
    [self onDisconnect];
}

- (void)webSocketDidOpen:(SRWebSocket *)webSocket {
    [self log:[NSString stringWithFormat:@"Connection opened."]];
    
}

- (void)webSocket:(SRWebSocket *)webSocket didFailWithError:(NSError *)error {
    NSLog(@"ERROR: Connection failed with error ... %@", [error localizedDescription]);
    // Assuming this resulted in a disconnect
    [self onDisconnect];
}

- (void)webSocket:(SRWebSocket *)webSocket didReceiveMessage:(NSString *)message {
    [self onData:message];
}

# pragma mark -

- (void) log:(NSString *)message 
{
#if DEBUG_LOGS
    NSLog(@"%@", message);
#endif
}

- (void) webSocketDispose_
{
    if(_webSocket == nil) return;
    [_webSocket close];
    _webSocket.delegate = nil;
    [[[SocketIO alloc] init] performSelector:@selector(webSocketDispose2_:) withObject:_webSocket afterDelay:30];
    _webSocket = nil;
}

- (void) webSocketDispose2_:(NSObject*)socket
{
    NSLog(@"disposing of socket: %@",socket);
}

- (void) dealloc
{
    [_timeout invalidate];
    _handshakeRequest.delegate = nil;
    [self webSocketDispose_];
}


@end


# pragma mark -
# pragma mark SocketIOPacket implementation

@implementation SocketIOPacket

@synthesize type, pId, name, ack, data, args, endpoint;

- (id) init
{
    self = [super init];
    if (self)
    {
        _types = [NSArray arrayWithObjects: @"disconnect", 
                  @"connect", 
                  @"heartbeat", 
                  @"message", 
                  @"json", 
                  @"event", 
                  @"ack", 
                  @"error", 
                  @"noop", 
                  nil];
    }
    return self;
}

- (id) initWithType:(NSString *)packetType
{
    self = [self init];
    if (self)
    {
        self.type = packetType;
    }
    return self;
}

- (id) initWithTypeIndex:(int)index
{
    self = [self init];
    if (self)
    {
        self.type = [self typeForIndex:index];
    }
    return self;
}

- (id) dataAsJSON
{
    return [self.data JSONValue];
}

- (NSNumber *) typeAsNumber
{
    int index = [_types indexOfObject:self.type];
    NSNumber *num = [NSNumber numberWithInt:index];
    return num;
}

- (NSString *) typeForIndex:(int)index
{
    return [_types objectAtIndex:index];
}

@end
