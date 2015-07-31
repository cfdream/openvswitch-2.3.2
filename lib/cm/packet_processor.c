
#include <config.h>
#include <arpa/inet.h>
#include "lib/unaligned.h"
#include "lib/dpif-provider.h"
#include "cm_output.h"
#include "packet_processor.h"
#include "condition_rotator.h"
#include "../../CM_testbed_code/public_lib/debug_config.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"
#include "../../CM_testbed_code/public_lib/general_functions.h"

uint8_t SRC_DST_MAC[6] = {0x7c, 0x7a, 0x91, 0x86, 0xb3, 0xe8};

uint32_t g_received_pkt_num = 0;
uint32_t cnt1 = 0;
uint32_t cnt2 = 0;
uint32_t cnt3 = 0;
uint32_t switch_recv_pkt_num[NUM_SWITCHES+1];
pthread_t interval_rotate_thread;
pthread_t condition_rotate_thread;

void cm_task_init(void){
    DEBUG("start: cm_task_init");
    // init data_warehouse 
    if (data_warehouse_init() != 0) {
        ERROR("FAIL:data_warehouse_init");
        return;
    }
    // interval rotate thread 
    if (pthread_create(&interval_rotate_thread, NULL, rotate_interval, NULL)) {
        ERROR("Failed: pthread_create rotate_interval");
        return;
    }

    // condition rotate thread
    if (pthread_create(&condition_rotate_thread, NULL, rotate_condition_buffers, NULL)) {
        ERROR("FAIL: pthread_create condition_rotate_thread.");
        return;
    }

    memset(switch_recv_pkt_num, 0, sizeof(switch_recv_pkt_num));
    DEBUG("end: cm_task_init");
}

bool packet_sampled(struct eth_header *eh) {
    if (!eh) {
        return false;
    }

    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = (struct vlan_eth_header*) (eh);
        /*
        char buf[100];
        snprintf(buf, 100, "veth_type:0x%04x, veth_tci:%d", vlan_eh->veth_type, vlan_eh->veth_tci);
        DEBUG(buf);
        */
        return vlan_eh->veth_tci & TAG_VLAN_VAL;
    }
    return false;
}


/**
* @brief 
*
* @param p_packet
* @param dpif
*
* @return >=0 switchid, -1: fail
*/
int get_switch_id(const struct dp_packet *p_packet, const struct dpif *dpif){
    if (p_packet == NULL || dpif == NULL || dpif->dpif_class == NULL) {
        return -1;
    }
    //get inport id for the pkt
    odp_port_t in_port = p_packet->md.in_port.odp_port;

    //get port infor from port id
    struct dpif_port port;
    dpif->dpif_class->port_query_by_number( dpif, in_port, &port);
    
    //get switch id from port infor
    int switch_id = atoi(port.name+1);

    //char buf[100];
    //snprintf(buf, 100, "in_port:%d, type:%s, name:%s, switch_id:%d", in_port, port.type, port.name, switch_id);
    //CM_DEBUG(switch_id, buf); 
    return switch_id;
}

void process(const struct dp_packet *p_packet, const struct dpif* dpif){
    if (p_packet == NULL || dpif == NULL) {
        return;
    }

    uint16_t ether_type;
    //pkt_len, allocated_len
    uint32_t pkt_len;  //bytes in use
    uint32_t allocated_len;  //allocated
    packet_t packet;
    char buf[200];

    //the number of packets received ++
    g_received_pkt_num++; 
    
    //init for conditional measurement
    if (g_received_pkt_num == 1) {
        cm_task_init();
    }

    //pkt_len, allocated_len
    pkt_len = p_packet->size_;  //bytes in use
    allocated_len = p_packet->allocated_;  //allocated
    packet.len = pkt_len; //allocated_len = pkt_len + 4 (from debugging)
    UNUSED(allocated_len);

    //----------l2 header
    cnt1++;
    struct eth_header *eh = dp_packet_l2(p_packet);
    if (eh == NULL) {
        return;
    }
    cnt2++;
    if (!is_eth_addr_expected(eh->eth_dst) || !is_eth_addr_expected(eh->eth_src)) {
        //MAC addressses not matched
        //snprintf(buf, 200, "mac:%02x-%02x-%02x-%02x-%02x-%02x", 
        //    eh->eth_dst[0], eh->eth_dst[1], eh->eth_dst[2], eh->eth_dst[3], eh->eth_dst[4], eh->eth_dst[5]);
        //CM_DEBUG(switch_id, buf);
        return;
    }
    
    //get ether_type
    ether_type = ntohs(eh->eth_type);
    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = dp_packet_l2(p_packet);
        ether_type = ntohs(vlan_eh->veth_next_type);
    }
    if (ether_type != ETH_TYPE_IP) {
        //not ip packet
        return;
    }
    cnt3++;

    //---------l3 header, srcip, dstip, protocol
    struct ip_header *nh= dp_packet_l3(p_packet);
    packet.srcip = ntohl_ovs(nh->ip_src);
    packet.dstip = ntohl_ovs(nh->ip_dst);
    packet.protocol = nh->ip_proto;

    //--------get switch id
    int switch_id = get_switch_id(p_packet, dpif);
    if (switch_id > NUM_SWITCHES) {
        ERROR("switch_id>12");
        return;
    }

    if (packet.protocol == 0x06) {
        packet.sampled = packet_sampled(eh);
        //TCP packet, all are normal packets
        //CM_DEBUG(switch_id, "normal packet"); 
        
        //--------l4 header, src_port, dst_port
        struct tcp_header* th = dp_packet_l4(p_packet);
        packet.src_port = ntohs(th->tcp_src);
        packet.dst_port = ntohs(th->tcp_dst);
        packet.seqid = ntohl_ovs(th->tcp_seq);

        /* process the normal packet */
        process_normal_packet(switch_id, &packet);
        
        /*
        if (ENABLE_DEBUG && packet.srcip == DEBUG_SRCIP && packet.dstip == DEBUG_DSTIP &&
            packet.src_port == DEBUG_SPORT && packet.dst_port == DEBUG_DPORT) {
                char src_str[100];
                char dst_str[100];
                ip_to_str(packet.srcip, src_str, 100);
                ip_to_str(packet.dstip, dst_str, 100);

                snprintf(buf, 200, "switch: flow[%s-%s-%u-%u-%u--len:%u-%u-switch_id:%d-pktid-%u-%u-%u-%u]", 
                    src_str, dst_str, 
                    packet.src_port, packet.dst_port,
                    packet.seqid, pkt_len, allocated_len,
                    switch_id, g_received_pkt_num,
                    cnt1, cnt2, cnt3);
                CM_DEBUG(switch_id, buf);
                DEBUG(buf);
        }
        */

        ++switch_recv_pkt_num[switch_id];
        if (!(switch_recv_pkt_num[switch_id] % NUM_PKTS_TO_DEBUG)) {
            snprintf(buf, 200, "pkt received:%d", switch_recv_pkt_num[switch_id]);
            CM_DEBUG(switch_id, buf);
        }
    } else if (packet.protocol == 0x11) {
        //UDP packet, all are condition packets
        //CM_DEBUG(switch_id, "condition pkt");
        process_condition_packet(switch_id, &packet);
    } else {
        //snprintf(buf, 200, "other pkt, protocol:0x%02x", packet.protocol);
        //CM_DEBUG(switch_id, buf);
    }
}

void process_normal_packet(int switch_id, packet_t* p_packet) {
    flow_src_t flow_key;
    flow_key.srcip = p_packet->srcip;

    /* 1. add flow's volume in groundtruth map */
    hashtable_kfs_vi_t* flow_volume_map =  data_warehouse_get_flow_volume_map(switch_id-1);
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
    DEBUG("packet sampled");
    hashtable_kfs_vi_fixSize_t* flow_sample_map = data_warehouse_get_flow_sample_map(switch_id-1);
    hashtable_kfs_vi_t* target_flow_map = data_warehouse_get_target_flow_map(switch_id-1);
    assert(flow_sample_map != NULL);
    int sample_volume = ht_kfs_vi_fixSize_get(flow_sample_map, &flow_key);
    if (sample_volume < 0) {
        sample_volume = 0;
    }
    sample_volume += p_packet->len;
    ht_kfs_vi_fixSize_set(flow_sample_map, target_flow_map, &flow_key, sample_volume);
}

void process_condition_packet(int switch_id, packet_t* p_packet) {
    DEBUG("start: process_condition_packet");
    //record the condition information of the flow in condition_flow_map
    //NOTE: received condition is stored in inactive condition_flow_map, will be switches by condition_rotate_thread
    //Refer to condition_rotator.c
    flow_src_t flow_key;
    flow_key.srcip = p_packet->srcip;

    //store in target map
    hashtable_kfs_vi_t* target_flow_map = data_warehouse_get_unactive_target_flow_map(switch_id-1);
    assert(target_flow_map != NULL);
    ht_kfs_vi_set(target_flow_map, &flow_key, 1);
    DEBUG("end: process_condition_packet");
}

uint32_t ntohl_ovs(ovs_16aligned_be32 x) {
    return ((((uint32_t)(x.hi) & 0x0000ff00) << 8) | 
        (((uint32_t)(x.hi) & 0x000000ff) << 24) | 
        (((uint32_t)(x.lo) & 0x0000ff00) >> 8) | 
        (((uint32_t)(x.lo) & 0x000000ff) << 8));
}

bool is_eth_addr_expected(uint8_t addr[ETH_ADDR_LEN]) {
    for (int i = 0; i < ETH_ADDR_LEN; ++i) {
        if (addr[i] ^ SRC_DST_MAC[i]) {
            return false;
        }
    }
    return true;
}
