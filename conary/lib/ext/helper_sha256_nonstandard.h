/*
 * Copyright (c) SAS Institute Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef _HELPER_SHA256_NONSTANDARD_H
#define _HELPER_SHA256_NONSTANDARD_H

#include <stdint.h>

typedef struct {
	uint32_t state[8];
	uint32_t length;
	uint32_t curlen;
	unsigned char buf[64];
}
sha256ns_hash_state;

void sha256ns_init(sha256ns_hash_state *md);
void sha256ns_copy(const sha256ns_hash_state *source, sha256ns_hash_state *dest);
void sha256ns_update(sha256ns_hash_state *md, const unsigned char *buf, int len);
void sha256ns_final(sha256ns_hash_state *md, unsigned char *hash);
void sha256ns_digest(const unsigned char *data, int len, unsigned char *digest);

#endif
