#ifndef PACKET_PROCESSOR_H
#define PACKET_PROCESSOR_H

#include <netinet/in.h>
#include "../CM_testbed_code/public_lib/packet.h"
#include "../CM_testbed_code/public_lib/flow.h"
#include "../CM_testbed_code/public_lib/condition.h"
#include "../CM_testbed_code/public_lib/debug_config.h"
#include "../CM_testbed_code/public_lib/debug_output.h"
#include "dpif.h"
#include "global_data.h"
#include "data_warehouse.h"

const uint32_t CONDITION_PACKET_DIP = 167837954; //10.1.1.2
const uint16_t CONDITION_PACKET_DPORT = 65535;   //65535

bool packet_sampled(struct eth_header *eh);
void process(struct dpif_execute *execute);
void process_normal_packet(packet_t* packet);
void process_condition_packet(packet_t* packet);

bool packet_sampled(struct eth_header *eh) {
    if (!eh) {
        return false;
    }

    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = (struct vlan_eth_header*) (eh);
        return vlan_eh->veth_tci & TAG_VLAN_VAL;
    }
    return false;
}

void process(struct dpif_execute *execute){
    //pkt_len, allocated_len
    uint32_t pkt_len;  //bytes in use
    uint32_t allocated_len;  //allocated

    packet_t packet;
    
    //the number of packets received ++
    g_received_pkt_num++; 
    
    //init for conditional measurement
    if (g_first_packet) {
        g_first_packet= false;
    }

    //pkt_len, allocated_len
    pkt_len = execute->packet->size_;  //bytes in use
    allocated_len = execute->packet->allocated_;  //allocated
    packet.len = allocated_len;

    //ethernet header
    struct eth_header *eh = dp_packet_l2(execute->packet);
    packet.sampled = packet_sampled(eh);

    //ip header
    struct ip_header *nh= dp_packet_l3(execute->packet);
    packet.srcip = get_16aligned_be32(&nh->ip_src);
    packet.dstip = get_16aligned_be32(&nh->ip_dst);
    packet.protocol = nh->ip_proto;

    if (packet.protocol == 0x06) {
        //TCP packet, all are normal packets
        struct tcp_header* th = dp_packet_l4(execute->packet);
        packet.src_port = ntohs(th->tcp_src);
        packet.dst_port = ntohs(th->tcp_dst);
        packet.seqid = get_16aligned_be32(&th->tcp_seq);
        //process the packet
        process_normal_packet(&packet);

        //debug one flow: set in public_lib/debug_config.h
        if (ENABLE_DEBUG && packet.srcip == DEBUG_SRCIP && packet.dstip == DEBUG_DSTIP &&
            packet.src_port == DEBUG_SPORT && packet.dst_port == DEBUG_DPORT) {
            struct in_addr src_addr;
            struct in_addr dst_addr;
            char src_str[100];
            char dst_str[100];
            src_addr.s_addr = htonl(packet.srcip);
            char* temp = inet_ntoa(src_addr);
            memcpy(src_str, temp, strlen(temp));
            dst_addr.s_addr = htonl(packet.dstip);
            temp = inet_ntoa(dst_addr);
            memcpy(dst_str, temp, strlen(temp));
            
            char buffer[1000];
            snprintf(buffer, 1000, "flow[%s-%s-%u-%u-%u]\n", 
                src_str, dst_str, packet.src_port, packet.dst_port, packet.seqid);
            DEBUG(buffer);
        }
    } else if (packet.protocol == 0x11) {
        //UDP packet, all are condition packets
        process_condition_packet(&packet);
    }
}

void process_normal_packet(packet_t* p_packet) {
    flow_src_t flow_key;
    flow_key.srcip = p_packet->srcip;

    /* 1. add flow's volume in groundtruth map */
    hashtable_kfs_vi_t* flow_volume_map =  data_warehouse_get_flow_volume_map();
    assert(flow_volume_map != NULL);
    int ground_truth_volume = ht_kfs_vi_get(flow_volume_map, &flow_key);
    if (ground_truth_volume < 0) {
        ground_truth_volume = 0;
    }
    ground_truth_volume += p_packet->len;
    ht_kfs_vi_set(flow_volume_map, &flow_key, ground_truth_volume);

    /* 2. add flow's vlume in sampled map if sampled */
    if (!p_packet->sampled) {
        //if the packet not sampled, ignore the packet
        return;
    }
    hashtable_kfs_vi_fixSize_t* flow_sample_map = data_warehouse_get_flow_sample_map();
    assert(flow_sample_map != NULL);
    int sample_volume = ht_kfs_vi_fixSize_get(flow_sample_map, &flow_key);
    if (sample_volume < 0) {
        sample_volume = 0;
    }
    sample_volume += p_packet->len;
    ht_kfs_vi_fixSize_set(flow_sample_map, &flow_key, sample_volume);
}

void process_condition_packet(packet_t* p_packet) {
    //record the condition information of the flow in condition_flow_map
    flow_src_t flow_key;
    flow_key.srcip = p_packet->srcip;

    //store in target map
    hashtable_kfs_vi_t* target_flow_map = data_warehouse_get_target_flow_map();
    assert(target_flow_map != NULL);
    ht_kfs_vi_set(target_flow_map, &flow_key, 1);
}

#endif
