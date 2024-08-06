# X is U-35 for table btree leaf pages or ((U-12)*64/255)-23 for index pages.
# M is always ((U-12)*32/255)-23.
# Let K be M+((P-M)%(U-4)).
# If P<=X then all P bytes of payload are stored directly on the btree page without overflow.
# If P>X and K<=X then the first K bytes of P are stored on the btree page and the remaining P-K bytes are stored on overflow pages.
# If P>X and K>X then the first M bytes of P are stored on the btree page and the remaining P-M bytes are stored on overflow pages.

U = 4096
X = U-35
M = ((U-12)*32/255)-23
P = 13057
K = M+((P-M)%(U-4))
if P<=X:
    print("All P bytes of payload are stored directly on the btree page without overflow.")
elif P>X and K<=X:
    print(f"The first {K} bytes of P are stored on the btree page and the remaining {P-K} bytes are stored on overflow pages.")
elif P>X and K>X:
    print(f"The first {M} bytes of P are stored on the btree page and the remaining {P-M} bytes are stored on overflow pages.")