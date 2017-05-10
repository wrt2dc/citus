session "s1"
step "s1a"
{
    SELECT 1 FROM master_add_node('localhost', 57637);
    SELECT 1 FROM master_add_node('localhost', 57638);
}

step "s1b"
{
	SELECT nodename, nodeport FROM pg_dist_node ORDER BY nodename, nodeport;
}

permutation "s1a" "s1b"
