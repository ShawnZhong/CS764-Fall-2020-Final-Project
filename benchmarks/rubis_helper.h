//
// Created by Libin Zhou on 4/22/20.
//

#ifndef DBX1000_RUBIS_HELPER_H
#define DBX1000_RUBIS_HELPER_H
#include "global.h"
#include "helper.h"

#endif //DBX1000_RUBIS_HELPER_H
uint64_t bid_key_h(uint64_t item_id, uint64_t user_id, uint64_t bid_id);
uint64_t item_key(uint64_t item_id);
uint64_t  buy_now_key_h(uint64_t buynow_id, uint64_t user1_id, uint64_t item_id );
uint64_t  zipf_rand(uint64_t thread_id, uint64_t min, uint64_t max, double skew);
uint64_t generate_bid_id();
uint64_t generate_bn_id();
uint64_t generate_date();