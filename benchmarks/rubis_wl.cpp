//
// Created by Libin Zhou on 4/12/20.
//
#include <rubis.h>
#include "global.h"
#include "helper.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpcc_const.h"
#include "rubis_const.h"
#include "tpcc_helper.h"
#include "rubis_helper.h"

RC rubis_wl::init_schema(const char * schema_file) {
    workload::init_schema(schema_file);
    t_item = tables["ITEM"];
    t_bid=tables["BID"];
    t_buynow=tables["BUYNOW"];


    i_item = indexes["ITEM_IDX"];
    i_bid=indexes["BID_IDX"];
    i_buynow=indexes["BN_IDX"];

    return RCOK;
}

RC rubis_wl::init_table() {

/******** fill in data ************/
// data filling process:
//item
//bids
//buynow
/**********************************/
//may need to declare this
   /* rubis_buffer = new drand48_data * [g_num_wh];
    pthread_t * p_thds = new pthread_t[g_num_wh - 1];
    for (uint32_t i = 0; i < g_num_wh - 1; i++)
        pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
    threadInitWarehouse(this);
    for (uint32_t i = 0; i < g_num_wh - 1; i++)
        pthread_join(p_thds[i], NULL);*/
   init_tab_item();
    init_tab_buynow();
    printf("RUBIS Data Initialization Complete!\n");

    return RCOK;
}

void rubis_wl::init_tab_item() {
    //TODO: figure out random value
    for(uint64_t i=1; i<=num_items;i++){
        row_t * row;
        uint64_t row_id;
        t_item->get_new_row(row, 0, row_id);
        row->set_primary_key(i);
        row->set_value(I_IID, i);
        uint64_t seller_id;
        row->set_value(I_SELLER, seller_id);
        row->set_value(I_RESERVE_PRICE,50);
        row->set_value(I_INITIAL_PRICE,50);
        uint64_t start_date = URand(1519074334, 1550610463,0);
        row->set_value(I_START_DATE, start_date);
        row->set_value(I_QUANTITY,10);
        row->set_value(I_NUM_OF_BIDS,10);
        row->set_value(I_MAX_BID,40);
        uint64_t end_date = URand(start_date, 1550610463,0);
        row->set_value(I_END_DATE,end_date);

        //insert index
        index_insert(i_item,item_key(i),row,0);

        //init bid
        init_tab_bid(i, start_date);

    }
}

void rubis_wl::init_tab_bid(uint64_t item_id, uint64_t start_date) {
    for(uint64_t i=0; i<num_bids_per_item; i++){
        uint64_t bid_id = generate_bid_id();
        row_t * row;
        uint64_t row_id;
        t_bid->get_new_row(row, 0, row_id);
        //TODO: find a way to get a bid key
        uint64_t user_id =zipf_rand(0,1,num_users,user_sigma);
        uint64_t bid_key = bid_key_h(item_id,user_id,bid_id);
        row->set_primary_key(bid_key);
        row->set_value(BID_ID, bid_id);
        row->set_value(BID_KEY, bid_key);
        row->set_value(BID_MAX_BID,40);
        row->set_value(BID_BID,40);
        row->set_value(BID_QUANTITY,1);
        uint64_t date = URand(start_date, 1550610463,0);
        row->set_value(BID_DATE,date);
        index_insert(i_bid,bid_key,row,0);
    }
}
 void rubis_wl::init_tab_buynow() {
    for(uint64_t i=0; i<buynow_prepop; i++){
        row_t * row;
        uint64_t row_id;
        t_buynow->get_new_row(row, 0, row_id);
        uint64_t user_id_1=zipf_rand(0,1,num_users,user_sigma);
        //TODO: this may neeed to be an item id
        uint64_t user_id_2=zipf_rand(0,1,num_users,user_sigma);
        uint64_t buynow_id=generate_bn_id();
        uint64_t buynow_key = buy_now_key_h(buynow_id,user_id_1,user_id_2);
        row->set_primary_key(buynow_key);
        row->set_value(BN_ID,buynow_id);
        row->set_value(BN_KEY,buynow_key);
        row->set_value(BN_QUANTITY,1);
        uint64_t date = URand(1519074334, 1550610463,0);
        row->set_value(BN_DATE,date);
        index_insert(i_buynow,buynow_key,row,0);
    }
}

