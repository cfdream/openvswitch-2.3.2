/*
 * NOTE: switch_id here should already be [0, NUM_SWITCHES)
 *
 * */

#include <config.h>
#include <arpa/inet.h>
#include "lib/unaligned.h"
#include "lib/dpif-provider.h"
#include "cm_output.h"
#include "packet_processor.h"
#include "packet_consecutive_drop_model.h"
#include "../../CM_testbed_code/public_lib/debug_config.h"
#include "../../CM_testbed_code/public_lib/debug_output.h"
#include "../../CM_testbed_code/public_lib/sample_packet.h"
#include "../../CM_testbed_code/public_lib/general_functions.h"
#include "../../CM_testbed_code/public_lib/cm_experiment_setting.h"

uint8_t SRC_DST_MAC[6] = {0x7c, 0x7a, 0x91, 0x86, 0xb3, 0xe8};

uint32_t g_received_pkt_num = 0;
uint32_t switch_recv_pkt_num[NUM_SWITCHES];
uint64_t switch_recv_volume[NUM_SWITCHES];
uint32_t switch_recv_condition_pkt_num[NUM_SWITCHES];

pthread_t interval_rotate_thread;
pthread_t condition_rotate_thread;

void cm_task_init(void){
    DEBUG("start: cm_task_init");

    if(init_cm_experiment_setting() != 0) {
        ERROR("FAIL: inti_cm_experiment_setting()");
        return;
    }

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

    /*
    // condition rotate thread
    if (pthread_create(&condition_rotate_thread, NULL, rotate_condition_buffers, NULL)) {
        ERROR("FAIL: pthread_create condition_rotate_thread.");
        return;
    }
    */

    init_packet_consecutive_drop_model();

    memset(switch_recv_pkt_num, 0, sizeof(switch_recv_pkt_num));
    memset(switch_recv_volume, 0, sizeof(switch_recv_volume));
    memset(switch_recv_condition_pkt_num, 0, sizeof(switch_recv_condition_pkt_num));
    DEBUG("end: cm_task_init");
}

int switch_packet_sampled(packet_t* p_packet, struct drand48_data* p_rand_buffer, int switch_id) {
    hashtable_kfs_fixSize_t* flow_sample_map = data_warehouse_get_flow_sample_map(switch_id);
    //hashtable_kfs_fixSize_t* target_flow_map = data_warehouse_get_target_flow_map(switch_id);
    //return sample_packet_fixSize_map(p_packet, p_packet->len, p_rand_buffer, flow_sample_map, target_flow_map);
    return sample_packet_fixSize_map(p_packet, p_packet->len, p_rand_buffer, flow_sample_map);
}

//0: not sampled, other: sampled
int host_packet_sampled(struct eth_header *eh) {
    if (!eh) {
        return 0;
    }

    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = (struct vlan_eth_header*) (eh);
        /*
        char buf[100];
        snprintf(buf, 100, "veth_type:0x%04x, sample_bit-veth_tci:%d", vlan_eh->veth_type, vlan_eh->veth_tci);
        DEBUG(buf);
        */
        return vlan_eh->veth_tci & TAG_VLAN_PACKET_SAMPLED_VAL;
    }
    return 0;
}

/*
bool get_target_flow_bit_val(struct eth_header *eh) {
    if(!eh) {
        return false;
    }

    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = (struct vlan_eth_header*) (eh);
        //char buf[100];
        //snprintf(buf, 100, "veth_type:0x%04x, target_flow_bit-veth_tci:%d", vlan_eh->veth_type, vlan_eh->veth_tci);
        //DEBUG(buf);
        return vlan_eh->veth_tci & TAG_VLAN_TARGET_FLOW_VAL;
    }
    return false;
}
*/

int get_selected_flow_level_val(struct eth_header *eh) {
    if(!eh) {
        return false;
    }

    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = (struct vlan_eth_header*) (eh);
        /*
        char buf[100];
        snprintf(buf, 100, "veth_type:0x%04x, target_flow_bit-veth_tci:%d", vlan_eh->veth_type, vlan_eh->veth_tci);
        DEBUG(buf);
        */
        return GET_VLAN_SELECTED_FLOW_VAL(vlan_eh->veth_tci);
    }
    return false;
}

bool is_the_switch_monitor_for_the_pkt(struct eth_header *eh, int switch_id) {
    if(!eh) {
        return false;
    }

    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = (struct vlan_eth_header*) (eh);
        /*
        char buf[100];
        snprintf(buf, 100, "veth_type:0x%04x, target_flow_bit-veth_tci:%04x", vlan_eh->veth_type, vlan_eh->veth_tci);
        CM_DEBUG(switch_id, buf);
        */
        return vlan_eh->veth_tci & TAG_VLAN_FOR_SWITCH_I(switch_id);
    }
    return false;
}

/**
* @brief 
*
* @param p_packet
* @param dpif
*
* @return >=0 [0, NUM_SENDER-1] switchid, -1: fail
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
    //FIX BUG: call dpif_port_destroy(), otherwise memory leakage
    dpif_port_destroy(&port);

    //char buf[100];
    //snprintf(buf, 100, "in_port:%d, type:%s, name:%s, switch_id:%d", in_port, port.type, port.name, switch_id);
    //CM_DEBUG(switch_id, buf); 
    return switch_id-1;
}

//return 0: packet not dropped, 1:packet dropped
int process(const struct dp_packet *p_packet, const struct dpif* dpif, struct drand48_data* p_rand_buffer) {
    if (p_packet == NULL || dpif == NULL) {
        return 0;
    }

    uint16_t ether_type;
    //pkt_len, allocated_len
    //uint32_t pkt_len;  //bytes in use
    //uint32_t allocated_len;  //allocated
    packet_t packet;
    //char buf[200];

    //the number of packets received ++
    g_received_pkt_num++; 
    
    //init for conditional measurement
    if (g_received_pkt_num == 1) {
        cm_task_init();
    }

    //pkt_len, allocated_len
    /*
    pkt_len = p_packet->size_;  //bytes in use
    allocated_len = p_packet->allocated_;  //allocated
    packet.len = pkt_len; //allocated_len = pkt_len + 4 (from debugging)
    UNUSED(allocated_len);
    */

    //----------l2 header
    struct eth_header *eh = dp_packet_l2(p_packet);
    if (eh == NULL) {
        return 0;
    }
    if (!is_eth_addr_expected(eh->eth_dst) || !is_eth_addr_expected(eh->eth_src)) {
        //MAC addressses not matched
        //snprintf(buf, 200, "mac:%02x-%02x-%02x-%02x-%02x-%02x", 
        //    eh->eth_dst[0], eh->eth_dst[1], eh->eth_dst[2], eh->eth_dst[3], eh->eth_dst[4], eh->eth_dst[5]);
        //CM_DEBUG(switch_id, buf);
        return 0;
    }
    
    //get ether_type
    ether_type = ntohs(eh->eth_type);
    if (eth_type_vlan(eh->eth_type)) {
        struct vlan_eth_header * vlan_eh = dp_packet_l2(p_packet);
        ether_type = ntohs(vlan_eh->veth_next_type);
    }
    if (ether_type != ETH_TYPE_IP) {
        //not ip packet
        return 0;
    }

    //---------l3 header, srcip, dstip, protocol
    struct ip_header *nh= dp_packet_l3(p_packet);
    packet.srcip = ntohl_ovs(nh->ip_src);
    packet.dstip = ntohl_ovs(nh->ip_dst);
    packet.protocol = nh->ip_proto;

    //--------get switch id
    int switch_id = get_switch_id(p_packet, dpif);
    if (switch_id >= NUM_SWITCHES) {
        ERROR("switch_id>12");
        return 0;
    }

    if (packet.protocol == 0x06) {
        //TCP packet, all are normal packets
        //CM_DEBUG(switch_id, "normal packet"); 

        //if packet is dropped, just return 1 (informing caller the packet is dropped)
        if(drop_packet(switch_id, p_rand_buffer)) {
            //packet dropped
            return 1;
        }
        
        //--------l4 header, src_port, dst_port
        struct tcp_header* th = dp_packet_l4(p_packet);
        packet.src_port = ntohs(th->tcp_src);
        packet.dst_port = ntohs(th->tcp_dst);
        packet.seqid = ntohl_ovs(th->tcp_seq);

        //--------payload: packet total length, including the packet header len
        // in tcpreplay, 4 bytes are used after the header to store the pkt len including the header len
        // refer to CM_testbed_code:fa97a12c8e946d624f597571eba5b06a8dd20519
        char* pkt_buf = (char*)dp_packet_l4(p_packet) + sizeof(struct tcp_header);
        packet.len = *(int*)pkt_buf;

        //---------pre process for normal packet, sample and hold, get target flow information---------
        //check whether the switch is one monitor for the packet
        bool is_monitor_for_the_pkt = is_the_switch_monitor_for_the_pkt(eh, switch_id);
        /*
        if (ENABLE_DEBUG && is_monitor_for_the_pkt) {
            char src_str[100];
            char dst_str[100];
            ip_to_str(packet.srcip, src_str, 100);
            ip_to_str(packet.dstip, dst_str, 100);

            snprintf(buf, 200, "switch: flow[%s-%s-%u-%u-%u--len:%u-switch_id:%d-pktid-%u]", 
                src_str, dst_str, 
                packet.src_port, packet.dst_port,
                packet.seqid, packet.len, 
                switch_id, g_received_pkt_num);
            CM_DEBUG(switch_id, buf);
        }
        */

        //check pakcet is sample or not
        packet.sampled = 0;
        if (cm_experiment_setting.host_or_switch_sample == HOST_SAMPLE) {
            if (is_monitor_for_the_pkt) {
                packet.sampled = host_packet_sampled(eh);
            }
        } else if (cm_experiment_setting.host_or_switch_sample == SWITCH_SAMPLE) {
            packet.sampled = switch_packet_sampled(&packet, p_rand_buffer, switch_id);
        }

        //if TAG_PKT_AS_CONDITION, get selected_level
        if (is_monitor_for_the_pkt
            && cm_experiment_setting.inject_or_tag_packet == TAG_PKT_AS_CONDITION) {
            packet.selected_level = get_selected_flow_level_val(eh);
        }

        /* packet not dropped, process the normal packet */
        process_normal_packet(switch_id, &packet);
        
        /*
        if (ENABLE_DEBUG && packet.srcip == DEBUG_SRCIP && packet.dstip == DEBUG_DSTIP &&
            packet.src_port == DEBUG_SPORT && packet.dst_port == DEBUG_DPORT) {
                char src_str[100];
                char dst_str[100];
                ip_to_str(packet.srcip, src_str, 100);
                ip_to_str(packet.dstip, dst_str, 100);

                snprintf(buf, 200, "switch: flow[%s-%s-%u-%u-%u--len:%u-switch_id:%d-pktid-%u]", 
                    src_str, dst_str, 
                    packet.src_port, packet.dst_port,
                    packet.seqid, packet.len, 
                    switch_id, g_received_pkt_num);
                CM_DEBUG(switch_id, buf);
                DEBUG(buf);
        }
        */
        //snprintf(buf, 200, "switch: flow[len1-%u-len2-%u-__packet_data-%u-%04x-%u ]", pkt_len, allocated_len, __packet_data(p_packet), packet.len, packet.len);
        //DEBUG(buf);
        
    } else if (packet.protocol == 0x11) {
        //UDP packet, all are condition packets
        //CM_DEBUG(switch_id, "condition pkt");
        
        //check pakcet is sample or not
        packet.selected_level = get_selected_flow_level_val(eh);

        process_condition_packet(switch_id, &packet);
    } else {
        //snprintf(buf, 200, "other pkt, protocol:0x%02x", packet.protocol);
        //CM_DEBUG(switch_id, buf);
    }
    return 0;
}

void process_normal_packet(int switch_id, packet_t* p_packet) {
    ++switch_recv_pkt_num[switch_id];
    switch_recv_volume[switch_id] += p_packet->len;
    ++data_warehouse.pkt_num_rece[data_warehouse.active_idx][switch_id];
    data_warehouse.volume_rece[data_warehouse.active_idx][switch_id] += p_packet->len;

    if (!(switch_recv_pkt_num[switch_id] % NUM_PKTS_TO_DEBUG)) {
        char buf[200];
        snprintf(buf, 200, "pkt received:%d, recv_volume:%ld", switch_recv_pkt_num[switch_id], switch_recv_volume[switch_id]);
        CM_DEBUG(switch_id, buf);
    }

    flow_src_t flow_key;
    #ifdef FLOW_SRC
    flow_key.srcip = p_packet->srcip;
    #endif
    #ifdef FLOW_SRC_DST
    flow_key.srcip = p_packet->srcip;
    flow_key.dstip = p_packet->dstip;
    #endif

    /* 1. add flow's volume in groundtruth map */
    hashtable_kfs_vi_t* flow_volume_map =  data_warehouse_get_flow_volume_map(switch_id);
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
    //CM_DEBUG(switch_id, "packet sampled");
    /* 3. for sampled packet, add the flow's volume in flow_sample_map */
    hashtable_kfs_fixSize_t* flow_sample_map = data_warehouse_get_flow_sample_map(switch_id);
    assert(flow_sample_map != NULL);

    if (cm_experiment_setting.inject_or_tag_packet == INJECT_PKT_AS_CONDITION) {
        //normal packet does not include selected_level information
        ht_kfs_fixSize_add_value(flow_sample_map, &flow_key, p_packet->len);
    } else if (cm_experiment_setting.inject_or_tag_packet == TAG_PKT_AS_CONDITION){
        //normal packet does include selected_level information
        ht_kfs_fixSize_add_value_and_update_selected_level_info(flow_sample_map, &flow_key, p_packet->len, p_packet->selected_level);
    }
}

void process_condition_packet(int switch_id, packet_t* p_packet) {
    ++switch_recv_condition_pkt_num[switch_id];
    ++data_warehouse.condition_pkt_num_rece[data_warehouse.active_idx][switch_id];
    if (!(switch_recv_condition_pkt_num[switch_id] % NUM_CONDITION_PKTS_TO_DEBUG)) {
        char buf[200];
        snprintf(buf, 200, "condition pkt received:%d", switch_recv_condition_pkt_num[switch_id]);
        CM_DEBUG(switch_id, buf);
    }

    //char buf[100];
    //snprintf(buf, 100, "condition srcip:%u, plus_minus:%d", p_packet->srcip, p_packet->is_target_flow);
    //CM_DEBUG(switch_id, buf);
    
    //record the condition information of the flow in condition_flow_map
    //NOTE: received condition is stored in inactive condition_flow_map, will be switches by condition_rotate_thread
    //Refer to condition_rotator.c
    flow_src_t flow_key;
    #ifdef FLOW_SRC
    flow_key.srcip = p_packet->srcip;
    #endif
    #ifdef FLOW_SRC_DST
    flow_key.srcip = p_packet->srcip;
    flow_key.dstip = p_packet->dstip;
    #endif

    //record the target flow info
    hashtable_kfs_fixSize_t* flow_sample_map = data_warehouse_get_flow_sample_map(switch_id);
    assert(flow_sample_map != NULL);
    ht_kfs_fixSize_set_selected_level(flow_sample_map, &flow_key, p_packet->selected_level);

    //store in all target map
    hashtable_kfs_vi_t* all_flow_selected_level_map = data_warehouse_get_all_flow_selected_level_map(switch_id);
    assert(all_flow_selected_level_map != NULL);
    if (p_packet->selected_level) {
        ht_kfs_vi_set(all_flow_selected_level_map, &flow_key, p_packet->selected_level);
    } else {
        ht_kfs_vi_del(all_flow_selected_level_map, &flow_key);
    }
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
