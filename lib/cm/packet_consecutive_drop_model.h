#ifndef __PACKET_CONSECUTIVE_DROP_MODEL_H__
#define __PACKET_CONSECUTIVE_DROP_MODEL_H__

void init_packet_consecutive_drop_model(void);
bool drop_packet(int switch_id, struct drand48_data* p_rand_buffer);

#endif
