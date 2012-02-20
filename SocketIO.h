//
//  SocketIO.h
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

#import <Foundation/Foundation.h>

@class SRWebSocket;
@class SocketIO;
@class SocketIOPacket;

typedef void(^SocketIOCallback)(id argsData);

@protocol SocketIODelegate <NSObject>
@optional
- (void) socketIODidConnect:(SocketIO *)socket;
- (void) socketIODidDisconnect:(SocketIO *)socket;
- (void) socketIO:(SocketIO *)socket didReceiveMessage:(SocketIOPacket *)packet;
- (void) socketIO:(SocketIO *)socket didReceiveJSON:(SocketIOPacket *)packet;
- (void) socketIO:(SocketIO *)socket didReceiveEvent:(SocketIOPacket *)packet;
- (void) socketIO:(SocketIO *)socket didSendMessage:(SocketIOPacket *)packet;
- (void) socketIOHandshakeFailed:(SocketIO *)socket;
@end


@interface SocketIO : NSObject 
{
@private
    NSString *_host;
    NSInteger _port;
    BOOL _secure;
    NSString *_sid;
    NSArray *_endpoints;
    NSMutableArray *_connectedEndpoints;
    NSString *_currentEndpoint;
    
    __unsafe_unretained id<SocketIODelegate> _delegate;
    
    SRWebSocket *_webSocket;
    
    BOOL _isConnected;
    BOOL _isConnecting;
    
    // heartbeat
    NSTimeInterval _heartbeatTimeout;
    NSTimer *_timeout;
    
    NSMutableArray *_queue;
    
    // acknowledge
    NSMutableDictionary *_acks;
    NSInteger _ackCount;
}

@property (nonatomic, readonly) BOOL isConnected, isConnecting;
@property (strong) NSString *currentEndpoint;
@property (assign) id<SocketIODelegate> delegate;

- (id) initWithDelegate:(id<SocketIODelegate>)delegate;
- (void) connectToHost:(NSString *)host onPort:(NSInteger)port secure:(BOOL)secure;
- (void) connectToHost:(NSString *)host onPort:(NSInteger)port secure:(BOOL)secure withParams:(NSDictionary *)params;
- (void) connectToHost:(NSString *)host onPort:(NSInteger)port secure:(BOOL)secure withParams:(NSDictionary *)params withNamespace:(NSString *)endpoint;
- (void) connectToHost:(NSString *)host onPort:(NSInteger)port secure:(BOOL)secure withParams:(NSDictionary *)params withNamespaces:(NSArray *)endpoints;
- (void) disconnect;

- (void) sendMessage:(NSString *)data;
- (void) sendMessage:(NSString *)data withAcknowledge:(SocketIOCallback)function;
- (void) sendJSON:(NSDictionary *)data;
- (void) sendJSON:(NSDictionary *)data withAcknowledge:(SocketIOCallback)function;
- (void) sendEvent:(NSString *)eventName withData:(NSDictionary *)data;
- (void) sendEvent:(NSString *)eventName withData:(NSDictionary *)data andAcknowledge:(SocketIOCallback)function;
- (void) sendAcknowledgement:(NSString*)pId withArgs:(NSArray *)data;

@end


@interface SocketIOPacket : NSObject
{
    NSString *type;
    NSString *pId;
    NSString *ack;
    NSString *name;
    NSString *data;
    NSArray *args;
    NSString *endpoint;
    
@private
    NSArray *_types;
}

@property (nonatomic, copy) NSString *type;
@property (nonatomic, copy) NSString *pId;
@property (nonatomic, copy) NSString *ack;
@property (nonatomic, copy) NSString *name;
@property (nonatomic, copy) NSString *data;
@property (nonatomic, copy) NSString *endpoint;
@property (nonatomic, copy) NSArray *args;

- (id) initWithType:(NSString *)packetType;
- (id) initWithTypeIndex:(int)index;
- (id) dataAsJSON;
- (NSNumber *) typeAsNumber;
- (NSString *) typeForIndex:(int)index;

@end