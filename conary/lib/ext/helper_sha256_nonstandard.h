/*
 * Copyright (c) rPath, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
