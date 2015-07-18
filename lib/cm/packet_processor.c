#include <config.h>
#include <arpa/inet.h>
#include "lib/unaligned.h"
#include "packet_processor.h"
#include "../../CM_testbed_code/public_lib/debug_config.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"
#include "../../CM_testbed_code/public_lib/general_functions.h"

VLOG_DEFINE_THIS_MODULE(packet_processor);

uint32_t g_received_pkt_num = 0;

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

pthread_t interval_rotate_thread;

void process(struct dpif_execute *execute){
    uint16_t ether_type;
    //pkt_len, allocated_len
    uint32_t pkt_len;  //bytes in use
    uint32_t allocated_len;  //allocated
    packet_t packet;
    char buf[200];
    
    //the number of packets received ++
    g_received_pkt_num++; 
    
    //*
    //init for conditional measurement
    if (g_received_pkt_num == 1) {

        // init data_warehouse 
        if (data_warehouse_init() != 0) {
            printf("FAIL:data_warehouse_init\n");
            return;
        }
        // interval rotate thread 
        if (pthread_create(&interval_rotate_thread, NULL, rotate_interval, NULL)) {
            printf("\nFailed: pthread_create rotate_interval\n");
            return;
        }
    }

    //pkt_len, allocated_len
    pkt_len = execute->packet->size_;  //bytes in use
    allocated_len = execute->packet->allocated_;  //allocated
    packet.len = allocated_len;

    /*
    struct vlan_eth_header *veh = dp_packet_l2(packet);
    if (veh->veth_type != ETH_TYPE_VLAN) {
        
    }
    */

    //l2 header
    if (execute->packet == NULL) {
        DEBUG("packet is NULL");
    }
    struct eth_header *eh = dp_packet_l2(execute->packet);
    if (eh == NULL) {
        DEBUG("eth = NULL");
        return;
    }
    ether_type = ntohs(eh->eth_type);
    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = dp_packet_l2(execute->packet);
        ether_type = ntohs(vlan_eh->veth_next_type);
        packet.sampled = packet_sampled(eh);
    }
    if (ether_type != ETH_TYPE_IP) {
        snprintf(buf, 200, "not ip pkt, ether_type:0x%04x", ether_type);
        DEBUG(buf);
        return;
    }

    //l3 header, srcip, dstip, protocol
    struct ip_header *nh= dp_packet_l3(execute->packet);
    packet.srcip = get_16aligned_be32(&nh->ip_src);
    packet.dstip = get_16aligned_be32(&nh->ip_dst);
    packet.protocol = nh->ip_proto;

    if (packet.protocol == 0x06) {
        //TCP packet, all are normal packets
        DEBUG("normal pkt");
        
        //l4 header, src_port, dst_port
        struct tcp_header* th = dp_packet_l4(execute->packet);
        packet.src_port = ntohs(th->tcp_src);
        packet.dst_port = ntohs(th->tcp_dst);
        packet.seqid = get_16aligned_be32(&th->tcp_seq);

        //process the packet
        //process_normal_packet(&packet);

        if (ENABLE_DEBUG && packet.srcip == DEBUG_SRCIP && packet.dstip == DEBUG_DSTIP &&
            packet.src_port == DEBUG_SPORT && packet.dst_port == DEBUG_DPORT) {
                char src_str[100];
                char dst_str[100];
                ip_to_str(packet.srcip, src_str, 100);
                ip_to_str(packet.dstip, dst_str, 100);

                snprintf(buf, 200, "switch: flow[%s-%s-%u-%u-%u-len:%u-%u]\n", 
                    src_str, dst_str, 
                    packet.src_port, packet.dst_port, packet.seqid, pkt_len, allocated_len);
                DEBUG(buf);
        }
    } else if (packet.protocol == 0x11) {
        //UDP packet, all are condition packets
        //process_condition_packet(&packet);
        DEBUG("condition pkt");
    } else {
        snprintf(buf, 200, "other pkt, protocol:0x%02x", packet.protocol);
        DEBUG(buf);
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
