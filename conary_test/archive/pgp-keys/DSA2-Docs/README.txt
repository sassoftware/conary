# Generated with:

mkdir -p /tmp/dsa2-home
chmod 700 /tmp/dsa2-home
gpg --import ../DSA2-Tests/keys/DSA-*

for key in DSA-1024-160 DSA-2048-224 DSA-3072-256 DSA-7680-384; do
    for algo in SHA384 SHA512; do
        gpg --homedir /tmp/dsa2-home --detach-sig -u $key --digest-algo $algo --output $key-$algo.doc.sig $key-$algo.doc
    done
done

for key in DSA-1024-160 DSA-2048-224 DSA-3072-256; do
    for algo in SHA256; do
        gpg --homedir /tmp/dsa2-home --detach-sig -u $key --digest-algo $algo --output $key-$algo.doc.sig $key-$algo.doc
    done
done


for key in DSA-1024-160 DSA-2048-224 ; do
    for algo in SHA224; do
        gpg --homedir /tmp/dsa2-home --detach-sig -u $key --digest-algo $algo --output $key-$algo.doc.sig $key-$algo.doc
    done
done

for key in DSA-1024-160; do
    for algo in SHA1; do
        gpg --homedir /tmp/dsa2-home --detach-sig -u $key --digest-algo $algo --output $key-$algo.doc.sig $key-$algo.doc
    done
done

