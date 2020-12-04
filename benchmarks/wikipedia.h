//
// Created by Libin Zhou on 5/16/20.
//

#ifndef DBX1000_WIKIPEDIA_H
#define DBX1000_WIKIPEDIA_H

#endif //DBX1000_WIKIPEDIA_H
#include <ctime>
#include <system/wl.h>
#include "txn.h"
#include "wl.h"

uint64_t anonymous_page_update_prob = 26;
uint64_t anonymous_user_id = 0;
uint64_t token_length = 32;
uint64_t pages = 1000;
uint64_t users = 2000;
uint64_t page_scale;
uint64_t user_scale;
uint64_t num_users = users*user_scale;
int* user_revision_counts;
int* page_last_rev_ids;
int* page_last_rev_lens;
uint64_t num_pages = pages*page_scale;
uint64_t max_watches_per_user = 1000;
uint64_t batch_size = 1000;
double num_watches_per_user_sigma = 1.75;
double page_id_sigma = 1.0001;
double watchlist_page_sigma = 1.0001;
double revision_user_sigma = 1.0001;
class table_t;
class INDEX;
class wikipedia_query;
class article{
public:
    int32_t text_id;
    int32_t page_id;
    int32_t rev_id;
    char* old_text;
    char*  user_text;
};

class wikipedia_wl: public workload{
public:
    RC init();
    RC init_table();
    RC init_schema(const char * schema_file);
    RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);

    table_t* t_ipblocks;
    table_t* t_logging;
    table_t* t_page;
    table_t* t_page_restriction;
    table_t* t_recent_changes;
    table_t* t_revision;
    table_t* t_text;
    table_t* t_user_acct;
    table_t* t_user_group;
    table_t* t_watchlist;

    INDEX* i_ipblocks;
    INDEX* i_page;
    INDEX* i_page_restriction;
    INDEX* i_user_acct;
    INDEX* i_watchlist;
    //TODO: check if we actually need it
    INDEX* i_revision;
    INDEX* i_text;
    INDEX* i_page_id;
private:
    void init_tab_ipblocks();
    void init_tab_logging();
    void init_tab_page();
    void init_tab_page_restriction();
    void init_tab_recent_changes();
    void init_tab_revision();
    void init_tab_text();
    void init_tab_user_acct();
    void init_tab_user_group();
    void init_tab_watchlist();
};

class wikipedia_txn_man:public txn_man{
public:
    void init(thread_t * h_thd, workload * h_wl, uint64_t thd_id);
    RC run_txn(base_query * query);
private:
    wikipedia_wl * _wl;
    RC run_add_watchlist(wikipedia_query * m_query);
    RC run_remove_watchlist(wikipedia_query * m_query);
    RC run_update_page(wikipedia_query * m_query);
    RC run_list_page_name_space(wikipedia_query * m_query);
    RC run_get_page_anon(wikipedia_query * m_query);
    RC run_get_page_auth(wikipedia_query * m_query);
    bool run_update_page_inner(wikipedia_query *m_query);
};