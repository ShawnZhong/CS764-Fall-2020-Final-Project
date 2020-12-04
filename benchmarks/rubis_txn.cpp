//
// Created by Libin Zhou on 5/12/20.
//
#include "rubis.h"
#include "rubis_query.h"
#include "tpcc_helper.h"
#include "rubis_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "rubis_const.h"

void rubis_txn_man::init(thread_t *h_thd, workload *h_wl,  uint64_t thd_id) {
    txn_man::init(h_thd, h_wl, thd_id);
    _wl = (rubis_wl *) h_wl;
}

RC rubis_txn_man::run_txn(base_query * query) {
    rubis_query * m_query = (rubis_query *) query;
    switch (m_query->type) {
        case Rubis_Place_Bid :
            return run_place_bid(m_query); break;
        case Rubis_Buy_Now :
            return run_buy_now(m_query); break;
        case Rubis_View_item :
            return run_view_item(m_query); break;
        default:
            assert(false);
    }
}

RC rubis_txn_man::run_place_bid(rubis_query *m_query) {
    uint64_t item_id =m_query-> item_id;
    uint64_t user_id =m_query-> user_id;
    uint64_t max_bid =m_query-> max_bid;
    uint64_t qty =m_query->qty;
    uint64_t bid =m_query->bid;
    INDEX* index = _wl->i_item;
    //update table item
    itemid_t* item = index_read(index,item_key(item_id),0);
    assert(item!=NULL);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local;
    r_wh_local = get_row(r_wh, WR);
    //what if row does not exist
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    uint64_t original_max_bid;
    r_wh_local->get_value(I_MAX_BID,original_max_bid);
    if(max_bid>original_max_bid){
        r_wh_local->set_value(I_MAX_BID, max_bid);
    }
    //update num bid
    uint64_t num_bid;
    r_wh_local->get_value(I_NUM_OF_BIDS,num_bid);
    num_bid+=1;
    r_wh_local->set_value(I_NUM_OF_BIDS,num_bid);

    //update bid table
    row_t * row;
    uint64_t row_id;
    //insert row
    if (!insert_row(row, _wl->t_bid, 0, row_id)) return Abort;
   // _wl->t_bid->get_new_row(row, 0, row_id);
    uint64_t bid_id =generate_bid_id();
    uint64_t bid_key = bid_key_h(item_id,user_id,bid_id);
    row->set_value(BID_ID,bid_id);
    row->set_value(BID_KEY,bid_key);
    row->set_primary_key(bid_key);
    row->set_value(BID_QUANTITY,qty);
    uint64_t date = generate_date();
    row->set_value(BID_DATE,date);
    //insert index
    auto idx = _wl->i_bid;
    if (!insert_idx(idx, bid_key, row, 0)) {
        return Abort;
    }
    return finish(RCOK);
}

RC rubis_txn_man::run_buy_now(rubis_query *m_query) {
    uint64_t user_id = m_query->user_id;
    uint64_t item_id = m_query->item_id;
    uint64_t qty = m_query->qty;
    uint64_t date = generate_date();
    INDEX* index = _wl->i_item;
    itemid_t* item = index_read(index,item_key(item_id),0);
    assert(item!=NULL);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local;
    r_wh_local = get_row(r_wh, WR);
    if (r_wh_local == NULL) {
        return finish(Abort);
    }
    uint64_t current_qty;
    r_wh_local->get_value(I_QUANTITY,current_qty);
    current_qty-=qty;
    if(current_qty==0){
        r_wh_local->set_value(I_END_DATE,date);
    }
    r_wh_local->set_value(I_QUANTITY,current_qty);
    //insert buy now row
    uint64_t buy_now_id = generate_bn_id();
    uint64_t buy_now_key=buy_now_key_h(buy_now_id,user_id,item_id);
    row_t * row;
    uint64_t row_id;
    if (!insert_row(row, _wl->t_buynow, 0, row_id)) return Abort;
    row->set_value(BN_ID,buy_now_index);
    row->set_value(BN_KEY,buy_now_key);
    row->set_primary_key(buy_now_key);
    row->set_value(BN_QUANTITY,qty);
    row->set_value(BN_DATE,date);
    //insert buy now index
    auto idx = _wl->i_buynow;

    if (!insert_idx(idx, buy_now_key, row, 0)) {
        return Abort;
    }
    return finish(RCOK);
}

RC rubis_txn_man::run_view_item(rubis_query *query) {
    uint64_t item_id = query->item_id;
    INDEX* index = _wl->i_item;
    itemid_t* item = index_read(index,item_key(item_id),0);
    assert(item!=NULL);
    row_t * r_wh = ((row_t *)item->location);
    row_t * r_wh_local;
    r_wh_local = get_row(r_wh, RD);
    //item should always exist
    if (r_wh_local == NULL) {
        return finish(Abort);
    }

    return finish(RCOK);
}