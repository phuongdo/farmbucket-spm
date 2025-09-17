#import "FarmHashWrapper.h"

#include "../vendor/FarmHash/farmhash.h"

@implementation FarmHashWrapper
+ (uint64_t)fingerprint64ForData:(NSData *)data {
    const char *bytes = (const char *)data.bytes;
    size_t len = (size_t)data.length;
    return util::Fingerprint64(bytes, len);
}
@end
