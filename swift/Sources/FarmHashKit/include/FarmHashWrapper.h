#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/// Thin ObjC(++) wrapper exposing a deterministic 64-bit fingerprint to Swift.
@interface FarmHashWrapper : NSObject
+ (uint64_t)fingerprint64ForData:(NSData *)data __attribute__((swift_name("fingerprint64(for:)")));
@end

NS_ASSUME_NONNULL_END
