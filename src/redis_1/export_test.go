package redis_1

func (c *baseClient) Pool() pool {
	return c.connPool
}
