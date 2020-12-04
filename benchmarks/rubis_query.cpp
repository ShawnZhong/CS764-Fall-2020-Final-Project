#include "query.h"
#include "rubis_query.h"
#include "rubis.h"
#include "rubis_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "tpcc_helper.h"

void rubis_query::init(uint64_t thd_id) {
    int b = URand(1,10,thd_id);
    if(b<4){
        gen_place_bid(thd_id);
    }else if(b<9){
        gen_view_item(thd_id);
    }else{
        gen_buy_now(thd_id);
    }
}

void rubis_query::gen_place_bid(uint64_t thd_id) {
    type = Rubis_Place_Bid;
    item_id=URand(1,num_items,thd_id);
    user_id=zipf_rand(thd_id,1,num_users,user_sigma);
    item_id=zipf_rand(thd_id,1,num_items,item_sigma);
    max_bid=40;
    qty=1;
    bid = max_bid + URand(1,10,thd_id) - 5;
}

void rubis_query::gen_buy_now(uint64_t thd_id) {
    type = Rubis_Buy_Now;
    qty=1;
    item_id=zipf_rand(thd_id,1,num_items,item_sigma);
    user_id=zipf_rand(thd_id,1,num_users,user_sigma);
}

void rubis_query::gen_view_item(uint64_t thd_id) {
    type = Rubis_View_item;
    item_id=zipf_rand(thd_id,1,num_items,item_sigma);
}