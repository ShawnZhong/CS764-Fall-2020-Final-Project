
#include "rubis_helper.h"
#include "rubis.h"
#include "atomic"
uint64_t bid_key_h(uint64_t item_id, uint64_t user_id, uint64_t bid_id){
    return (item_id*13/5+user_id*9/4+bid_id*12/7);
}
uint64_t item_key(uint64_t item_id){
    return item_id;
}
uint64_t  buy_now_key_h(uint64_t buynow_id, uint64_t user1_id, uint64_t item_id){
    return (buynow_id*8+user1_id*3+item_id*5);
}
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
uint64_t generate_bid_id(){
    return std::atomic_fetch_add(&bid_index,(uint64_t)1);
}
uint64_t generate_bn_id(){
    return std::atomic_fetch_add(&buy_now_index,(uint64_t)1);
}
//come from sto::Rubis_bench.hh::uint32_t input_generator::generate_date();
uint64_t generate_date(){
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto n = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    return static_cast<uint64_t>(n);
}