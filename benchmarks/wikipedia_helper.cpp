//
// Created by Libin Zhou on 5/20/20.
//
#include "wikipedia_helper.h"
#include "wikipedia.h"
#include "atomic"
#include "tpcc_helper.h"
// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug.
// The original paper says zeta(theta, 2.0). But I guess it should be
// zeta(2.0, theta).
double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++)
        sum += pow(1.0 / i, theta);
    return sum;
}

uint64_t zipf(uint64_t n, double theta) {
    // assert(this->the_n == n);
    double alpha = 1 / (1 - theta);
    double zetan = zeta(n,theta);
    double eta = (1 - pow(2.0 / n, 1 - theta)) /
                 (1 - zeta(2, theta) / zetan);
    double u = drand48();
    //drand48_r(&_query_thd->buffer, &u);
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, theta)) return 2;
    return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}
uint64_t  zipf_rand(uint64_t thread_id, uint64_t min, uint64_t max, double skew){
    return min+zipf((max-min),skew);
}

uint64_t generate_page_id(){
    //return a random page id
    return zipf_rand(0,1,num_pages,page_id_sigma);
}

//TODO: the followings are random num distributions from histogram
uint64_t generate_user_name_length(){

}

uint64_t generate_user_real_name_length(){

}
uint64_t generate_page_title_length(){

}
uint64_t generate_num_revisions(){

}
uint64_t generate_text_length(){

}
//TODO: other distributions from histogram
uint64_t generate_page_ns(uint64_t page_id){

}
char* generate_page_restrictions(){

}
uint64_t generate_rev_minor_edit(){

}
//end here
uint64_t generate_user_id(){
    //return a random user id
    return URand(1,num_users,0);
}

//atomic generation of unique IDs
uint64_t generate_rev_key(){
    return std::atomic_fetch_add(&rev_index,(uint64_t)1);
};

uint64_t generate_text_key(){
    return std::atomic_fetch_add(&text_index,(uint64_t)1);
}

uint64_t generate_log_key(){
    return std::atomic_fetch_add(&log_index,(uint64_t)1);
}

uint64_t generate_rc_key(){
    return std::atomic_fetch_add(&rc_index,(uint64_t)1);
}

uint64_t generate_ip_part(){
    return URand(0,255,0);
}

char generate_random_char(){
    char a= URand(36,126,0);
    return a;
}

char* generate_user_ip(){
    char* base ="";
    for(int i=0; i<3;i++){
        uint64_t local=generate_ip_part();
        std::string s = std::to_string(local);
        char const *pchar = s.c_str();
        char* coma =".";
        strncat(base,pchar,strlen(pchar));
        strncat(base,coma,strlen(coma));
    }
    uint64_t local=generate_ip_part();
    std::string s = std::to_string(local);
    char const *pchar = s.c_str();
    strncat(base,pchar,strlen(pchar));
    return base;
}
char* generate_random_string(int len){
    char* base="";
    for(int i=0; i<len;i++){
        char toAdd = generate_random_char();
        strncat(base,&toAdd,1);
    }
    return base;
}

char* generate_name(){
    return generate_random_string(generate_user_name_length());
}
char* generate_real_name(){
    return generate_random_string(generate_user_real_name_length());
}
char* generate_user_token(){
    return generate_random_string(token_length);
}
char* generate_time(){
    auto t = std::time(nullptr);
    std::tm* tm = std::localtime(&t);
    return std::asctime(tm);
}
//TODO: many need to check if we need seed
char* generate_page_title(uint64_t page_id){
    uint64_t title_length = generate_page_title_length();
    char* base="";
    for(int i=0; i<title_length;i++){
        char toAdd = generate_random_char();
        strncat(base,&toAdd,1);
    }
    return base;
}
float generate_page_random(){
    //get from stackoverflow https://stackoverflow.com/questions/686353/random-float-number-generation
    float r2 = static_cast <float> (rand()) / (static_cast <float> (RAND_MAX/100.0));
    return r2;
}
uint64_t generate_page_key(uint64_t page_ns, char* page_title){
    int len = strlen(page_title);
    int sum=0;
    for(int i=0; i<len;i++){
        sum+=page_title[i]-'0';
    }
    return sum+page_ns*1000000;
}

uint64_t generate_page_key_with_id(uint64_t page_id){
    return page_id;
}

uint64_t generate_num_watches(){
    return zipf_rand(0,0,std::min(num_pages,max_watches_per_user),num_watches_per_user_sigma);
}

uint64_t generate_watchlist_key(int uid, int ns, char* title){
    int sum=0;
    int len = strlen(title);
    for(int i=0; i<len;i++){
        sum+=title[i]-'0';
    }
    return uid*1000000+sum+ns*1000;
}

char* generate_old_text(){
    int len = generate_text_length();
    return generate_random_string(len);
}
//TODO: may need extra work here
char* generate_rev_text(char* old_text){
    return "a good rev text this is";
}
uint64_t text_key_c(uint64_t text_id){
    return text_id;
}
uint64_t rev_key_c(uint64_t rev_id){
    return rev_id;
}
char* generate_rev_comment(){
    return "this is a comment";
}
uint64_t user_key_c(uint64_t user_id){
    return user_id;
}
uint64_t generate_big_page_key(uint64_t page_ns){
    int sum =127*200;
    return sum+page_ns*1000000;
}
