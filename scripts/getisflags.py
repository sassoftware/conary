#!/usr/bin/python
#
# Copyright (c) 2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import subprocess

fpu = set(('f2xm1', 'fabs', 'fadd', 'faddp', 'fiadd', 'fbld', 'fchs',
           'fclex', 'fnclex', 'fcom', 'fcomp', 'fcompp', 'ficom',
           'ficomp', 'fcos', 'fdecstp', 'fdisi', 'fndisi', 'fdiv',
           'fdivp', 'fidiv', 'fdivr', 'fdivrp', 'fidivr', 'feni',
           'fneni', 'ffree', 'fiadd', 'fisub', 'fisubr', 'fimul',
           'fidiv', 'fidivr', 'ficom', 'ficomp', 'fild', 'fincstp',
           'finit', 'fninit', 'fist', 'fistp', 'fld', 'fild', 'fbld',
           'fld1', 'flz', 'fldpi', 'fldl2e', 'fldl2t', 'fldlg2',
           'fldln2', 'fldcw', 'fldenv', 'fldenvw', 'fldenvd', 'fmul',
           'fmulp', 'fimul', 'fnop', 'fpatan', 'fprem', 'fprem1',
           'fptan', 'frdndint', 'frstor', 'frstorw', 'frstord',
           'fsave', 'fsavew', 'fsaved', 'fnsave', 'fnsavew',
           'fnsaved', 'fscale', 'fsetpm', 'fsin', 'fsincos', 'fsqrt',
           'fst', 'fstp', 'fist', 'fistp', 'fbstp', 'fstcw', 'fnstcw',
           'fstenv', 'fstenvw', 'fstenvd', 'fnstenv', 'fnstenvw',
           'fnstenvd', 'fstsw', 'fnstsw', 'fsub', 'fsubp', 'fisub',
           'fsubr', 'fsubrp', 'fisubr', 'ftst', 'fucom', 'fucomp',
           'fucompp', 'fwait', 'fxam', 'fxch', 'fxtract', 'fyl2x',
           'fyl2xp1'))

# although cpuid is in some i486 chips, we're going to require i586
# to be safe
i486 = set(('bswap', 'cmpxchg', 'invd', 'invlpg', 'xadd', 'wbinvd'))
# force anything that uses fpu to 486, which included a FPU built in
i486.update(fpu)

i586 = set(('cpuid', 'rdmsr', 'rdtsc', 'wrmsr'))

cpuid = set(('cpuid',))

i686 = set(('fcmova', 'fcmovae', 'fcmovb', 'fcmovbe', 'fcmove',
            'fcmovna', 'fcmovnae', 'fcmovnb', 'fcmovnbe', 'fcmovne',
            'fcmovnu', 'fcmovu', 'fcomi', 'fcomip', 'ud2'))

sep = set(('sysenter', 'sysexit'))

clflush = set(('clflush',))

monitor = set(('monitor', 'mwait'))

fxsr = set(('fxsave', 'fxrstor'))

cx8 = set(('cmpxchg8b',))

cx16 = set(('cmpxchg16b',))

cmov = set(('cmova', 'cmovae', 'cmovb', 'cmovbe', 'cmovc', 'cmove',
            'cmovg' 'cmovge', 'cmovel', 'cmovle', 'cmovna', 'cmovnae',
            'cmovnb', 'cmovnbe', 'cmovnc', 'cmovne', 'cmovng',
            'cmovnge', 'cmovnl', 'cmovnle', 'cmovno', 'cmovnp',
            'cmovns', 'cmovnz', 'cmovo', 'cmovp', 'cmovpe', 'cmovpo',
            'cmovs', 'cmovz'))

mmx = set(('rdpmc', 'emms', 'movd', 'movq', 'packsswb', 'packssdw',
           'packuswb', 'paddb', 'paddw', 'paddd', 'paddsb', 'paddsw',
           'paddusb', 'paddusw', 'pand', 'pandn', 'pcmpeqb',
           'pcmpeqw', 'pcmpeqd', 'pcmpgtb', 'pcmpgtw', 'pcmpgtd',
           'pmaddwd', 'pmulhw', 'pmullw', 'por', 'psllw', 'pslld',
           'psllq', 'psraw', 'psrad', 'psrlw', 'psrld', 'psrlq',
           'psubb', 'psubw', 'psubd', 'psubsb', 'psubsw', 'psubusb',
           'psubusw', 'punpckhbw', 'punpckhwd', 'punpckhdq',
           'punpcklbw', 'punpcklwd', 'punpckldq', 'pxor'))

# FIXME: include mmxext?

_3dnow = set(('pavgusb', 'pfadd', 'pfsub', 'pfsubr', 'pfacc',
              'pfcmpge', 'pfcmpgt', 'pfcmpeq', 'pfmin', 'pfmax',
              'pi2fw', 'pi2fd', 'pf2iw', 'pf2id', 'pfrcp', 'pfrsqrt',
              'pfmul', 'pfrcpit1', 'pfrsqit1', 'pfrcpit2', 'pmulhrw',
              'pswapw', 'femms'))

ext3dnow = set(('pf2iw', 'pfnacc', 'pfpnacc', 'pi2fw', 'pswapd',
                'maskmovq', 'movntq', 'pavgb', 'pavgw', 'pextrw',
                'pinsrw', 'pmaxsw', 'pmaxub', 'pminsw', 'pminub',
                'pmovmskb', 'pmulhuw', 'prefetchnta', 'prefetcht0',
                'prefetcht1', 'prefetcht2', 'psadbw', 'pshufw',
                'sfence'))

prefetch = set(('prefetch',))

sse = set(('addps', 'addss', 'andnps', 'andps', 'cmpps', 'cmpss',
           'comiss', 'cvtpi2ps', 'cvtps2pi', 'cvtsi2ss', 'cvtss2si',
           'cvttps2pi', 'cvttss2si', 'divps', 'divss', 'ldmxcsr',
           'maxps', 'maxss', 'minps', 'minss', 'movaps', 'movhlps',
           'movhps', 'movlhps', 'movlps', 'movmskps', 'movss',
           'movups', 'mulps', 'mulss', 'orps', 'pavgb', 'pavgw',
           'psadbw', 'rcpps', 'rcpss', 'rsqrtps', 'rsqrtss', 'shufps',
           'sqrtps', 'sqrtss', 'stmxcsr', 'subps', 'subss', 'ucomiss',
           'unpckhps', 'unpcklps', 'xorps', 'pextrw', 'pinsrw',
           'pmaxsw', 'pmaxub', 'pminsw', 'pminub', 'pmovmskb',
           'pmulhuw', 'pshufw', 'maskmovq', 'movntps', 'movntq',
           'sfence'))

sse2 = set(('addpd', 'addsd', 'andnpd', 'andpd', 'cmppd', 'cmpsd',
            'comisd', 'cvtdq2pd', 'cvtdq2ps', 'cvtpd2pi', 'cvtpd2pq',
            'cvtpd2ps', 'cvtpi2pd', 'cvtps2dq', 'cvtps2pd',
            'cvtsd2si', 'cvtsd2ss', 'cvtsi2sd', 'cvtss2sd',
            'cvttpd2pi', 'cvttpd2dq', 'cvttps2dq', 'cvttsd2si',
            'divpd', 'divsd', 'lfence', 'maskmovdqu', 'maxpd',
            'maxsd', 'mfence', 'minpd', 'minsd', 'movapd', 'movd',
            'movdq2q', 'movdqa', 'movdqu', 'movhpd', 'movlpd',
            'movmskpd', 'movntdq', 'movnti', 'movntpd', 'movq',
            'movq2dq', 'movsd', 'movupd', 'mulpd', 'mulsd', 'orpd',
            'packsswb', 'packssdw', 'packuswb', 'paddb', 'paddw',
            'paddd', 'paddq', 'paddq', 'paddsb', 'paddsw', 'paddusb',
            'paddusw', 'pand', 'pandn', 'pause', 'pavgb', 'pavgw',
            'pcmpeqb', 'pcmpeqw', 'pcmpeqd', 'pcmpgtb', 'pcmpgtw',
            'pcmpgtd', 'pextrw', 'pinsrw', 'pmaddwd', 'pmaxsw',
            'pmaxub', 'pminsw', 'pminub', 'pmovmskb', 'pmulhw',
            'pmulhuw', 'pmullw', 'pmuludq', 'pmuludq', 'por',
            'psadbw', 'pshufd', 'pshufhw', 'pshuflw', 'pslldq',
            'psllw', 'pslld', 'psllq', 'psraw', 'psrad', 'psrldq',
            'psrlw', 'psrld', 'psrlq', 'psubb', 'psubw', 'psubd',
            'psubq', 'psubq', 'psubsb', 'psubsw', 'psubusb',
            'psubusw', 'psubsb', 'punpckhbw', 'punpckhwd',
            'punpckhdq', 'punpckhqdq', 'punpcklbw', 'punpcklwd',
            'punpckldq', 'punpcklqdq', 'pxor', 'shufpd', 'sqrtpd',
            'sqrtsd', 'unpckhpd', 'xorpd'))

sse3 = set(('fisttp', 'addsubps', 'addsubpd', 'movsldup', 'movshdup',
            'movddup', 'lddqu', 'haddps', 'hsubps', 'haddpd',
            'hsubpd'))

InstructionSets = {
    '3dnow': _3dnow,
    '3dnowext': ext3dnow,
    'clflush': clflush,
    'cmov': cmov,
    'cpuid': cpuid,
    'cx16': cx16,
    'cx8': cx8,
    'fxsr': fxsr,
    'i486': i486,
    'i586': i586,
    'i686': i686,
    'mmx': mmx,
    'monitor': monitor,
    'prefetch': prefetch,
    'sep': sep,
    'sse': sse,
    'sse2': sse2,
    'sse3': sse3,
    }

def getIsFlags(path):
    isnsets = InstructionSets.copy()
    flags = set()
    p = subprocess.Popen('objdump -d %s' %path, stdout=subprocess.PIPE,
                         shell=True)
    for line in p.stdout:
        # first split the interesting lines into 3 chunks
        chunks = line.split('\t')
        if len(chunks) < 3:
            continue
        # grab the instruction
        isn = chunks[2].split(' ', 1)[0]
        for name, isnset in isnsets.items():
            if isn in isnset:
                # once we know we have one, there is no need to continue
                # to check
                del isnsets[name]
                flags.add(name)

    # if the application uses cpuid, it's possible that it will choose
    # the correct instructions for the CPU running it.
    if 'cpuid' in flags:
        strength = '~%s'
        flags.remove('cpuid')
    else:
        strength = '%s'
    return ','.join(sorted(strength %x for x in flags))

if __name__ == '__main__':
    def usage():
        print 'usage: %s binary' %sys.argv[0]
        sys.exit(1)

    import sys
    if len(sys.argv) != 2:
        usage()
    flavoring = getIsFlags(sys.argv[1])
    print flavoring
