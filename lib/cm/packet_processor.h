#ifndef PACKET_PROCESSOR_H
#define PACKET_PROCESSOR_H

#include "../CM_testbed_code/public_lib/packet.h"
#include "../CM_testbed_code/public_lib/flow.h"
#include "../CM_testbed_code/public_lib/condition.h"
#include "dpif.h"
#include "data_warehouse.h"
#include "interval_rotator.h"

#define CONDITION_PACKET_DIP 167837954 //10.1.1.2
#define CONDITION_PACKET_DPORT 65535   //65535

bool packet_sampled(struct eth_header *eh);
void process(const struct dp_packet *packet, const struct dpif* dpif);
void process_normal_packet(packet_t* packet);
void process_condition_packet(packet_t* packet);

uint32_t ntohl_ovs(ovs_16aligned_be32 x);
bool is_eth_addr_expected(uint8_t addr[ETH_ADDR_LEN]);
int get_switch_id(const struct dp_packet *packet, const struct dpif *dpif);
#endif
