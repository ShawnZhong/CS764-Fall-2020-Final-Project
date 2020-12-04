//
// Created by Libin Zhou on 5/16/20.
//

#ifndef DBX1000_WIKIPEDIA_HELPER_H
#define DBX1000_WIKIPEDIA_HELPER_H
#include "global.h"
#include "helper.h"
#endif //DBX1000_WIKIPEDIA_HELPER_H
std::atomic<uint64_t> rev_index(1);
std::atomic<uint64_t> text_index(1);
std::atomic<uint64_t> log_index(1);
std::atomic<uint64_t> rc_index(1);
char* generate_name();
char* generate_real_name();
char* generate_time();
char* generate_user_token();
uint64_t generate_page_ns(uint64_t page_id);
char* generate_page_title(uint64_t page_id);
char* generate_page_restrictions();
float generate_page_random();
uint64_t generate_page_key(uint64_t page_ns, char* page_title);
uint64_t generate_page_key_with_id(uint64_t page_id);
uint64_t generate_num_watches();
uint64_t generate_page_id();
uint64_t generate_watchlist_key(int uid, int ns, char* title);
uint64_t generate_num_revisions();
char* generate_old_text();
uint64_t generate_rev_key();
uint64_t generate_text_key();
uint64_t generate_user_id();
char* generate_rev_text(char* old_text);
uint64_t text_key_c(uint64_t text_id);
uint64_t rev_key_c(uint64_t rev_id);
char* generate_rev_comment();
uint64_t generate_rev_minor_edit();
char* generate_user_ip();
uint64_t user_key_c(uint64_t user_id);
uint64_t generate_big_page_key(uint64_t page_ns);
uint64_t generate_log_key();
uint64_t generate_rc_key();


