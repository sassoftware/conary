/*
 * Copyright (c) 2011 rPath, Inc.
 *
 * This program is distributed under the terms of the Common Public License,
 * version 1.0. A copy of this license should have been distributed with this
 * source file in a file called LICENSE. If it is not present, the license
 * is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * without any warranty; without even the implied warranty of merchantability
 * or fitness for a particular purpose. See the Common Public License for
 * full details.
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
