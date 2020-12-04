//
// Created by Libin Zhou on 5/17/20.
//

#ifndef DBX1000_WIKIPEDIA_QUERY_H
#define DBX1000_WIKIPEDIA_QUERY_H

#endif //DBX1000_WIKIPEDIA_QUERY_H
#include "global.h"
#include "helper.h"
#include "query.h"
#include "wikipedia.h"

class wikipedia_query : public base_query{

public:
    void init(uint64_t thd_id);
    WikipediaTxnType type;
    /**********************************************/
    // common txn input for place bid and buynow and view item
    /**********************************************/
    char* t_type;
    uint64_t user_id;
    uint64_t page_id;
    uint64_t page_ns;
   // uint64_t text_id;
    char* page_title;
    bool for_select;
    char* user_ip;
    article art;
    size_t rt1;

private:
    void gen_add_watchlist(uint64_t thd_id);
    void gen_remove_watchlist(uint64_t thd_id);
    void gen_get_page_anon(uint64_t thd_id);
    void gen_get_page_auth(uint64_t thd_id);
    void gen_list_page_namespace(uint64_t thd_id);
    void gen_update_page(uint64_t thd_id);

};

