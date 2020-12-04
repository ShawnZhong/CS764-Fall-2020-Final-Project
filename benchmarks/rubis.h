//
// Created by Libin Zhou on 4/12/20.
//
#include <ctime>
#include <system/wl.h>
#include "txn.h"
#include "wl.h"

#ifndef DBX1000_RUBIS_H
#define DBX1000_RUBIS_H
size_t num_items = 500000;
size_t num_users = 100000;
//uint64_t bid_index=0;
std::atomic<uint64_t>bid_index(0);
std::atomic<uint64_t> buy_now_index(0);
double user_sigma = 0.2;
double item_sigma = 0.8;
size_t num_bids_per_item = 10;
size_t buynow_prepop = 500000;
#endif //DBX1000_RUBIS_H
class table_t;
class INDEX;
class rubis_query;

class rubis_wl: public workload{
public:
    RC init();
    RC init_table();
    //TODO:check init schema
    RC init_schema(const char * schema_file);
    RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
    table_t *		t_item;
    table_t*        t_bid;
    table_t*        t_buynow;

    //TODO: what index do i need to set?
    INDEX*          i_item;
    INDEX*          i_bid;
    INDEX*          i_buynow;
    uint32_t next_tid;
private:
    void init_tab_item();
    void init_tab_bid(uint64_t item_id, uint64_t  start_date);
    void init_tab_buynow();

};

class rubis_txn_man : public txn_man
{
public:
    void init(thread_t * h_thd, workload * h_wl, uint64_t thd_id);
    RC run_txn(base_query * query);
private:
   rubis_wl * _wl;
    RC run_place_bid(rubis_query * m_query);
    RC run_buy_now(rubis_query * m_query);
    RC run_view_item(rubis_query * query);
};