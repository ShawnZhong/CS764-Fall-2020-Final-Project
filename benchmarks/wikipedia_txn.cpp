//
// Created by Libin Zhou on 5/18/20.
//
#include "wikipedia.h"
#include "wikipedia_query.h"
#include "wikipedia_helper.h"
#include "wikipedia_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "wikipedia_const.h"
void wikipedia_txn_man::init(thread_t *h_thd, workload *h_wl,  uint64_t thd_id) {
    txn_man::init(h_thd, h_wl, thd_id);
    _wl = (wikipedia_wl *) h_wl;
}

RC wikipedia_txn_man::run_txn(base_query *query) {
    wikipedia_query* m_query=(wikipedia_query*) query;
    switch(m_query->type){
        case Wikipedia_Add_Watchlist:
            return run_add_watchlist(m_query); break;
        case Wikipedia_Remove_Watchlist:
            return run_remove_watchlist(m_query); break;
        case Wikipedia_Get_Page_Anon:
            return run_get_page_anon(m_query);break;
        case Wikipedia_Get_Page_Auth:
            return run_get_page_auth(m_query);
        case Wikipedia_List_Page_Namespace:
            return run_list_page_name_space(m_query);
        case Wikipedia_Update_Page:
            return run_update_page(m_query);
        default:
            assert(false);
    }
}

RC wikipedia_txn_man::run_add_watchlist(wikipedia_query *m_query) {
    uint64_t user_id = m_query->user_id;
    uint64_t page_ns = m_query->page_ns;
    char* page_title = m_query->page_title;
    row_t * row;
    uint64_t row_id;
    //insert row
    if (!insert_row(row, _wl->t_watchlist, 0, row_id)) return Abort;
    row->set_value(WL_UID,user_id);
    row->set_value(WL_TITLE,page_title);
    if(page_ns==0){
        row->set_value(WL_NS,1);
    }else{
        row->set_value(WL_NS,page_ns);
    }
    //insert index
    int watchlist_key=generate_watchlist_key(user_id,page_ns,page_title);
    auto idx = _wl->i_watchlist;
    if (!insert_idx(idx, watchlist_key, row, 0)) {
        return Abort;
    }

    //update user acct
    //TODO: how to read without index?
    char* user_touched = generate_time();
    auto index = _wl->i_user_acct;
    itemid_t* item = index_read(index,user_id,0);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local;
    r_wh_local = get_row(r_wh, WR);
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    r_wh_local->set_value(UA_USER_TOUCHED,user_touched);
    return finish(RCOK);
}

RC wikipedia_txn_man::run_remove_watchlist(wikipedia_query *m_query) {
    auto index = _wl->i_watchlist;
    auto key = generate_watchlist_key(m_query->user_id,m_query->page_ns,m_query->page_title);
    itemid_t* item = index_read(index,key,0);
    row_t * r_wh = ((row_t *)item->location);
    assert(item!=NULL);
    if (!remove_idx(index, key, (row_t *)item->location, 0)) return Abort;
    if (!remove_row(((row_t *)item->location))) return Abort;

    char* user_touched = generate_time();
    auto ua_index = _wl->i_user_acct;
    itemid_t* ua_item = index_read(index,m_query->user_id,0);
    row_t* r_wh_ua = ((row_t *)ua_item->location);
    row_t * r_wh_local;
    r_wh_local = get_row(r_wh_ua, WR);
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    r_wh_local->set_value(UA_USER_TOUCHED,user_touched);
    return finish(RCOK);
}

RC wikipedia_txn_man::run_get_page_anon(wikipedia_query *m_query) {
    bool for_select = m_query->for_select;
    char* user_ip = m_query->user_ip;
    uint64_t page_ns = m_query->page_ns;
    char* title = m_query->page_title;
    uint64_t page_key = generate_page_key(page_ns,title);
    auto index = _wl->i_page;
    itemid_t* item = index_read(index,page_key,0);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local;
    r_wh_local = get_row(r_wh, RD);
    //what if row does not exist
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    uint64_t page_id ;
     r_wh_local->get_value(P_PID,page_id);
     //select from revision with matching key
     uint64_t rev_id;
     r_wh_local->get_value(P_PAGE_LATEST,rev_id);
    uint64_t rev_key = rev_key_c(rev_id);
    index = _wl->i_revision;
    item = index_read(index,rev_key,0);
     r_wh = ((row_t *)item->location);
    r_wh_local;
    r_wh_local = get_row(r_wh, RD);
    //what if row does not exist
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    uint64_t rev_text_id;
    r_wh_local->get_value(R_TEXT_ID,rev_text_id);

    //select text row with matching rev text id
    index = _wl->i_text;
    uint64_t text_key = text_key_c(rev_text_id);
    item = index_read(index,text_key,0);
    r_wh = ((row_t *)item->location);
    r_wh_local;
    r_wh_local = get_row(r_wh, RD);
    //what if row does not exist
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    uint64_t art_text_id=rev_text_id;
    //r_wh_local->get_value(T_TID,art_text_id);
    uint64_t art_page_id=page_id;
    uint64_t art_rev_id = rev_id;
    char* art_old_text=r_wh_local->get_value(T_OLD_TEXT);
    char* art_user_text=user_ip;

    m_query->art.page_id=art_page_id;
    m_query->art.text_id=art_text_id;
    m_query->art.rev_id=art_rev_id;
    m_query->art.old_text=art_old_text;
    m_query->art.user_text=art_user_text;
    return finish(RCOK);

}

RC wikipedia_txn_man::run_get_page_auth(wikipedia_query *m_query) {
    RC rc = RCOK;
    bool for_select = m_query->for_select;
    uint64_t user_id = m_query->user_id;
    char* user_ip = m_query->user_ip;
    char* title = m_query->page_title;
    uint64_t page_ns = m_query->page_ns;
    auto index = _wl->i_user_acct;
    itemid_t* item = index_read(index,user_key_c(user_id),0);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local;
    r_wh_local = get_row(r_wh, RD);
    //what if row does not exist
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    auto page_key = generate_page_key(page_ns,title);
    index = _wl->i_page;
    //switch to search
    r_wh_local=search(index,page_key,0,RD);
    uint64_t page_id;
    r_wh_local->get_value(P_PID,page_id);

    uint64_t rev_id;
    r_wh_local->get_value(P_PAGE_LATEST,rev_id);
    auto rev_key = rev_key_c(rev_id);
    index = _wl->i_revision;
    r_wh_local = search(index,rev_key,0,RD);
    uint64_t rev_text_id;
    r_wh_local->get_value(R_TEXT_ID,rev_text_id);

    auto text_key = text_key_c(rev_text_id);
    index = _wl->i_text;
    r_wh_local=search(index,text_key,0,RD);

    //update article
    m_query->art.page_id=page_id;
    m_query->art.rev_id=rev_id;
    m_query->art.text_id=rev_text_id;
    m_query->art.user_text=user_ip;
    char* old_text = r_wh_local->get_value(T_OLD_TEXT);
    m_query->art.old_text=old_text;
    return finish(rc);
}

RC wikipedia_txn_man::run_list_page_name_space(wikipedia_query *m_query) {
    uint64_t page_ns = m_query->page_ns;
    INDEX* index = _wl->i_page;
    std::vector<std::pair<uint64_t , char*>> pages;
    /*
    auto scan_callback = [&](row_t* row) {
        uint64_t id;
        row->get_value(P_PID,id);
        char* title;
        title=row->get_value(P_PAGE_TITLE);
        pages.push_back({id,title});
        //pages.push_back({row.page_id, std::string(key.page_title.c_str())});
        return true;
    };*/
    itemid_t* items[20];
    uint64_t count = 20;
    auto min_key = generate_page_key(page_ns,"");
    auto max_key =generate_big_page_key(page_ns);
    auto idx_rc = index_read_range(index, min_key, max_key, items, count, 0);
    if (idx_rc == Abort) return Abort;
    assert(idx_rc == RCOK);
    for(int i=0; i<count; i++){
        auto shared = items[i];
        auto local = get_row((row_t *)shared->location, RD);
        if (local == NULL) return Abort;
        char* title1=local->get_value(P_PAGE_TITLE);
        uint64_t local_page_id;
        local->get_value(P_PID,local_page_id);

        //get comparison row
        INDEX* page_id_index =_wl->i_page_id;
        uint64_t page_id_key=generate_page_key_with_id(local_page_id);
        row_t* com_row = search(page_id_index,page_id_key,0,RD);
        char* title2 = com_row->get_value(P_PAGE_TITLE);
        if(strcmp(title1,title2)!=0){
            return Abort;
        }
    }
    return finish(RCOK);
}

bool wikipedia_txn_man:: run_update_page_inner(wikipedia_query *m_query){
    article local_art = m_query->art;
    uint64_t text_id =local_art.text_id;
    uint64_t page_id = local_art.page_id;
    char* page_title = m_query->page_title;
    char* page_text=generate_rev_text(local_art.old_text);
    uint64_t page_ns = m_query->page_ns;
    uint64_t user_id = m_query->user_id;
    char* user_ip =m_query->user_ip;
    char* user_text = local_art.user_text;
    uint64_t rev_id = local_art.rev_id;
    char* rev_comment = generate_rev_comment();
    uint64_t rev_minor_edit = generate_rev_minor_edit();
    char* current_time = generate_time();
    uint64_t new_text_id = generate_text_key();
    //row and index insertion
    row_t* row = NULL;
    uint64_t row_id;
    if (!insert_row(row, _wl->t_text, 0, row_id)) return false;
    row->set_value(T_TID,text_key_c(new_text_id));
    row->set_primary_key(text_key_c(new_text_id));
    row->set_value(T_OLD_PAGE,page_id);
    char* old_flags="utf-8";
    row->set_value(T_OLD_FLAGS,old_flags);
    char* new_old_text = new char[strlen(page_text) + 1];
    memcpy(new_old_text,page_text,strlen(page_text)+1);
    auto index = _wl->i_text;
    if (!insert_idx(index, text_key_c(new_text_id), row, 0)) {
        return false;
    }

    //revision row insert
    uint64_t new_rev_id = generate_rev_key();
    uint64_t new_rev_key=rev_key_c(new_rev_id);
     row = NULL;
     row_id;
    if (!insert_row(row, _wl->t_revision, 0, row_id)) return false;
    row->set_value(R_RID,new_rev_key);
    row->set_primary_key(new_rev_key);
    row->set_value(R_PAGE,page_id);
    //TODO: may need extra work here
    row->set_value(R_TEXT_ID,new_text_id);
    row->set_value(R_COMMENT,rev_comment);
    row->set_value(R_MINOR_EDIT,rev_minor_edit);
    row->set_value(R_USER,user_id);
    row->set_value(R_USER_TEXT,user_text);
    row->set_value(R_TIMESTAMP,current_time);
    row->set_value(R_DELETEED,0);
    uint64_t page_len =strlen(page_text);
    row->set_value(R_LEN,page_len);
    row->set_value(R_PARENT_ID,rev_id);
    index = _wl->i_revision;
    if (!insert_idx(index, new_rev_key, row, 0)) {
        return false;
    }

    //page row update
    index = _wl->i_page_id;
    uint64_t page_id_key = generate_page_key_with_id(page_id);
    index = _wl->i_page_id;
    row_t* page_row=search(index,page_id_key,0,WR);
    page_row->set_value(P_PAGE_LATEST,new_rev_id);
    page_row->set_value(P_PAGE_TOUCHED,current_time);
    page_row->set_value(P_PAGE_IS_NEW,0);
    page_row->set_value(P_PAGE_IS_REDIRECT,0);
    page_row->set_value(P_PAGE_LEN,page_len);

    //recent change row update
    uint64_t rc_key=generate_rc_key();
    row = NULL;
    row_id;
    if (!insert_row(row, _wl->t_revision, 0, row_id)) return false;
    row->set_value(RC_KEY,rc_key);
    row->set_primary_key(rc_key);
    row->set_value(RC_TIMESTAMP,current_time);
    row->set_value(RC_CURRENT_TIME,current_time);
    row->set_value(RC_NAMESPACE,page_ns);
    row->set_value(RC_TITLEE,page_title);
    row->set_value(RC_TYPE,0);
    row->set_value(RC_MINOR,0);
    row->set_value(RC_CUR_ID,page_id);
    row->set_value(RC_USER,user_id);
    row->set_value(RC_USER_TEXT,user_text);
    row->set_value(RC_COMMENT,rev_comment);
    row->set_value(RC_THIS_OLDID,new_text_id);
    row->set_value(RC_LAST_OLDID,text_id);
    row->set_value(RC_BOT,0);
    row->set_value(RC_MOVED_TO_NS,0);
    char* new_title = "";
    row->set_value(RC_MOVED_TO_TITLE,new_title);
    row->set_value(RC_IP,user_ip);
    row->set_value(RC_OLD_LEN,page_len);
    row->set_value(RC_NEW_LEN,page_len);

    //select watching users
    uint64_t small_watchlist_key = generate_watchlist_key(0,page_ns,page_title);
    uint64_t large_watchlist_key=generate_watchlist_key(std::numeric_limits<int32_t>::max(),page_ns,page_title);
    itemid_t* items[num_users];
    uint64_t count = num_users;
    auto idx_rc = index_read_range(index, small_watchlist_key, large_watchlist_key, items, count, 0);
    if (idx_rc == Abort) return false;
    assert(idx_rc == RCOK);
    for(int i=0; i<count;i++){
        auto shared = items[i];
        auto local = get_row((row_t *)shared->location, WR);
        //if (local == NULL) return false;
        local->set_value(WL_NOTIFICATION_TIMESTAMP,current_time);
    }

    //insert log
    uint64_t log_key = generate_log_key();
    row = NULL;
    row_id;
    if (!insert_row(row, _wl->t_logging, 0, row_id)) return false;
    char* type = "patrol";
    row->set_value(L_KEY,log_key);
    row->set_primary_key(log_key);
    row->set_value(L_TYPE,type);
    char* action="patrol";
    row->set_value(L_ACTION,action);
    row->set_value(L_TIMESTAMP,current_time);
    row->set_value(L_USER,user_id);
    row->set_value(L_NAMESPACE,page_ns);
    row->set_value(L_TITLE,page_title);
    row->set_value(L_COMMENT,rev_comment);
    char* empty_param="";
    row->set_value(L_PARAMS,empty_param);
    row->set_value(L_DELETED,0);
    row->set_value(L_USER_TEXT,user_text);
    row->set_value(L_PAGE,page_id);

    //update user
    index = _wl->i_user_acct;
    uint64_t user_key=user_key_c(user_id);
    row_t* user_row = search(index,user_key,0,WR);
    user_row->set_value(UA_USER_TOUCHED,current_time);
    uint64_t edit_count;
    user_row->get_value(UA_USER_EDIYCOUNT,edit_count);
    edit_count++;
    user_row->set_value(UA_USER_EDIYCOUNT,edit_count);

    return true;
}

RC wikipedia_txn_man::run_update_page(wikipedia_query *m_query) {
    run_get_page_anon(m_query);
    while(true){
        bool success = run_update_page_inner(m_query);
        if(success)break;
    }
    return finish(RCOK);
}





