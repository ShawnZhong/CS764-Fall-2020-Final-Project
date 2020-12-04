//
// Created by Libin Zhou on 5/17/20.
//
#include "query.h"
#include "wikipedia_query.h"
#include "wikipedia.h"
#include "wikipedia_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "tpcc_helper.h"

void wikipedia_query::init(uint64_t thd_id) {
    int b = URand(1,1000000,thd_id);
    if(b<0.07*10000){
        gen_add_watchlist(thd_id);
    }else if(b<0.14*10000){
        gen_remove_watchlist(thd_id);
    }else if(b<4.8125*10000){
        gen_update_page(thd_id);
    }else if(b<52.2343*10000){
        gen_list_page_namespace(thd_id);
    }else if(b<99.0781*10000){
        gen_get_page_anon(thd_id);
    }else{
        gen_get_page_auth(thd_id);
    }
}

void wikipedia_query::gen_add_watchlist(uint64_t thd_id) {
    user_id=generate_user_id();
    page_ns = generate_page_ns(page_id);
    page_title=generate_page_title(page_id);
}

void wikipedia_query::gen_remove_watchlist(uint64_t thd_id) {
    user_id=generate_user_id();
    page_id=generate_page_id();
    page_ns = generate_page_ns(page_id);
    page_title=generate_page_title(page_id);
}

void wikipedia_query::gen_get_page_anon(uint64_t thd_id) {
    for_select=false;
    user_ip=generate_user_ip();
    page_id=generate_page_id();
    page_ns = generate_page_ns(page_id);
    page_title=generate_page_title(page_id);

}

void wikipedia_query::gen_get_page_auth(uint64_t thd_id) {
    for_select=false;
    user_ip=generate_user_ip();
    user_id=generate_user_id();
    page_id=generate_page_id();
    page_ns = generate_page_ns(page_id);
    page_title=generate_page_title(page_id);
}

void wikipedia_query::gen_list_page_namespace(uint64_t thd_id) {
    page_id=generate_page_id();
    page_ns = generate_page_ns(page_id);
}

void wikipedia_query::gen_update_page(uint64_t thd_id) {
    user_ip = generate_user_ip();
    page_id=generate_page_id();
    page_ns = generate_page_ns(page_id);
    page_title=generate_page_title(page_id);
}