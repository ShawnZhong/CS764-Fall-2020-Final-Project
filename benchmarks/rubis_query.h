//
// Created by Libin Zhou on 4/15/20.
//

#ifndef DBX1000_RUBIS_QUERY_H
#define DBX1000_RUBIS_QUERY_H

#endif //DBX1000_RUBIS_QUERY_H
#include "global.h"
#include "helper.h"
#include "query.h"


class rubis_query : public base_query{
public:
    void init(uint64_t thd_id);
    RubisTxnType type;
    /**********************************************/
    // common txn input for place bid and buynow and view item
    /**********************************************/
    uint64_t item_id;
    uint64_t user_id;
    uint32_t max_bid;
    uint32_t qty;
    uint32_t bid;


private:
    void gen_place_bid(uint64_t thd_id);
    void gen_buy_now(uint64_t thd_id);
    void gen_view_item(uint64_t thd_id);

};