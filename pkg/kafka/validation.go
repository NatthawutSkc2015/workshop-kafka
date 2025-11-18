package kafka

import "errors"

// ============================================================================
// Validation
// ============================================================================

func (c *ProducerConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers cannot be empty")
	}
	if c.Topic == "" {
		return errors.New("topic cannot be empty")
	}
	if c.WriteTimeout < 0 {
		return errors.New("writeTimeout cannot be negative")
	}
	if c.ReadTimeout < 0 {
		return errors.New("readTimeout cannot be negative")
	}
	return nil
}

func (c *ConsumerConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers cannot be empty")
	}
	if c.Topic == "" {
		return errors.New("topic cannot be empty")
	}
	if c.GroupID == "" {
		return errors.New("groupID cannot be empty")
	}
	if c.MaxRetries < 0 {
		return errors.New("maxRetries cannot be negative")
	}
	if c.HandlerTimeout <= 0 {
		return errors.New("handlerTimeout must be greater than zero")
	}
	if c.MaxWait < 0 {
		return errors.New("maxWait cannot be negative")
	}
	return nil
}
