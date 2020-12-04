//
// Created by Libin Zhou on 5/16/20.
//

#include "wikipedia.h"
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
#include "wikipedia_const.h"
#include "tpcc_helper.h"
#include "wikipedia_helper.h"

RC wikipedia_wl::init_schema(const char * schema_file) {
    workload::init_schema(schema_file);
    t_ipblocks = tables["IPBLOCKS"];
    t_logging=tables["LOGGING"];
    t_page=tables["PAGE"];
    t_page_restriction=tables["PAGE_RESTRICTION"];
    t_recent_changes=tables["RECENT_CHANGES"];
    t_revision=tables["REVISION"];
    t_text=tables["TEXT"];
    t_user_acct=tables["USER_ACCT"];
    t_user_group=tables["USER_GROUP"];
    t_watchlist=tables["WATCHLIST"];


    i_ipblocks = indexes["IPBLOCKS_IDX"];
    i_page=indexes["PAGE_IDX"];
    i_page_restriction=indexes["PAGE_RESTRICTION_IDX"];
    i_user_acct=indexes["USER_ACCT"];
    i_watchlist=indexes["WATCHLIST"];
    return RCOK;
}

RC wikipedia_wl::init_table() {
    user_revision_counts=new int[num_users];
    for(int i=0; i<num_users;i++){
        user_revision_counts[i]=0;
    }
    page_last_rev_ids = new int[num_pages];
    page_last_rev_lens=new int[num_pages];
    for(int j=0; j<num_pages;j++){
        page_last_rev_ids[j]=0;
        page_last_rev_lens[j]=0;
    }
    init_tab_user_acct();
    init_tab_watchlist();
    init_tab_page();
    init_tab_revision();
    printf("WIKI{EDIA Data Initialization Complete!\n");
    return RCOK;
}

void wikipedia_wl::init_tab_user_acct() {
    for(int i=1; i<=num_users;i++){
        row_t * row;
        uint64_t row_id;
        t_user_acct->get_new_row(row, 0, row_id);
        row->set_value(UA_UID,i);
        row->set_primary_key(i);
        row->set_value(UA_USER_NAME,generate_name());
        row->set_value(UA_REAL_NAME,generate_real_name());
        char* pass = "password";
        row->set_value(UA_PASSWORD,pass);
        char* new_pass = "newpassword";
        row->set_value(UA_NEWPASSWORD,new_pass);
        row->set_value(UA_NEWPASS_TIME,generate_time());
        char* email ="user@example.com";
        row->set_value(UA_USER_EMAIL,email);
        char* user_option = "fake_longoptionslist";
        row->set_value(UA_USER_OPTIONS,user_option);
        row->set_value(UA_USER_TOUCHED,generate_time());
        row->set_value(UA_USER_TOKEN,generate_user_token());
        char* email_authenticated = "null";
        char* email_token="null";
        row->set_value(UA_EMAIL_TOKEN,email_token);
        row->set_value(UA_USER_EMAIL_AUTHENTICATED,email_authenticated);
        char* user_token_expires="null";
        row->set_value(UA_USER_EMAIL_TOKEN_EXPIRES,user_token_expires);
        char* user_regsitration="null";
        row->set_value(UA_USER_REGISTRATION,user_regsitration);
        row->set_value(UA_USER_EDIYCOUNT,user_revision_counts[i-1]);
        index_insert(i_user_acct,user_key_c(i),row,0);
    }
}

void wikipedia_wl::init_tab_page(){
    for(int i=1; i<=num_pages;i++){
        row_t * row;
        uint64_t row_id;
        t_page->get_new_row(row, 0, row_id);
        uint64_t page_ns = generate_page_ns(i);
        char* page_title = generate_page_title(i);
        auto page_restrictions = generate_page_restrictions();
        row->set_value(PID,i);
        row->set_primary_key(i);
        row->set_value(P_PAGE_NAMESPACE,page_ns);
        row->set_value(P_PAGE_TITLE,page_title);
        row->set_value(P_PAGE_RESTRICTIONS,page_restrictions);
        row->set_value(P_PAGE_IS_REDIRECT,0);
        row->set_value(P_PAGE_IS_NEW,0);
        row->set_value(P_PAGE_COUNTER,0);
        row->set_value(P_PAGE_RANDOM,generate_page_random());
        row->set_value(P_PAGE_TOUCHED,generate_time());
        row->set_value(P_PAGE_LATEST,page_last_rev_ids[i - 1]);
        row->set_value(P_PAGE_LEN,page_last_rev_lens[i - 1]);

        //insert index
        uint64_t page_key = generate_page_key(page_ns,page_title);
        index_insert(i_page,page_key,row,0);
        //do i need this extra index
        uint64_t page_id_key =generate_page_key_with_id(i);
        index_insert(i_page_id,page_id_key,row,0);
    }
}

void wikipedia_wl::init_tab_watchlist() {
    std::set<int> user_pages;
    for(int i=1; i<=num_users;i++){
        user_pages.clear();
        int num_watches = generate_num_watches();
        for(int j=1; j<=num_watches;j++){
            int page_id;
            if(num_watches==max_watches_per_user){
                page_id=j+1;
            }else{
                page_id = generate_page_id();
                if (user_pages.find(page_id) == user_pages.end()) {
                    break;
                }
            }
            user_pages.insert(page_id);
            uint64_t page_ns = generate_page_ns(page_id);
            char* page_title = generate_page_title(page_id);
            int watchlist_key = generate_watchlist_key(i,page_ns,page_title);
            row_t * row;
            uint64_t row_id;
            t_watchlist->get_new_row(row, 0, row_id);
            //TODO:check if right columns in row
            row->set_value(WL_UID,i);
            row->set_value(WL_NS,page_ns);
            row->set_value(WL_TITLE,page_title);
            char* timestamp="null";
            row->set_value(WL_NOTIFICATION_TIMESTAMP,timestamp);

            //insert indexes
            index_insert(i_watchlist,watchlist_key,row,0);
        }
    }
}

void wikipedia_wl::init_tab_revision() {
    for(int pid=1; pid<=num_pages;pid++){
        int num_revs = generate_num_revisions();
        char* old_text=generate_old_text();
        size_t old_text_length = strlen(old_text);
        for(int i=0;i<num_revs;i++){
            uint64_t tr_id=generate_rev_key();
            uint64_t tx_id=generate_text_key();
            always_assert(tx_id == tr_id, "text_id and rev_id should be the same at load time");
            uint64_t user_id = generate_user_id();
            ++user_revision_counts[user_id - 1];
            if(i>0){
                old_text=generate_rev_text(old_text);
                old_text_length=strlen(old_text);

            }
            uint64_t text_key = text_key_c(tx_id);
            //text row
            row_t * row;
            uint64_t row_id;
            t_text->get_new_row(row, 0, row_id);
            char* new_old_text = new char[old_text_length+1];
            memcpy(new_old_text,old_text,old_text_length+1);
            char* old_flags = "utf-8";
            row->set_primary_key(text_key);
            row->set_value(T_TID,text_key);
            row->set_value(T_OLD_TEXT,old_text);
            row->set_value(T_OLD_FLAGS,old_flags);
            row->set_value(T_OLD_PAGE,pid);
            index_insert(i_text,text_key,row,0);

            //revision row
            row_t * r_row;
            uint64_t r_row_id;
            t_revision->get_new_row(r_row, 0, r_row_id);
            uint64_t revision_key = rev_key_c(tr_id);
            r_row->set_value(R_RID,revision_key);
            r_row->set_primary_key(revision_key);
            r_row->set_value(R_PAGE,pid);
            //TODO: check correctness
            r_row->set_value(R_TEXT_ID,tr_id);
            r_row->set_value(R_COMMENT,generate_rev_comment());
            r_row->set_value(R_USER,user_id);
            char* user_text="I am a good user";
            r_row->set_value(R_USER_TEXT,user_text);
            r_row->set_value(R_TIMESTAMP,generate_time());
            r_row->set_value(R_MINOR_EDIT,generate_rev_minor_edit());
            r_row->set_value(R_DELETEED,0);
            r_row->set_value(R_LEN,(int)old_text_length);
            r_row->set_value(R_PARENT_ID,0);
            index_insert(i_revision,revision_key,r_row,0);
            page_last_rev_ids[pid - 1] = tr_id;
            page_last_rev_lens[pid - 1] = tr_id;
        }

    }
}

