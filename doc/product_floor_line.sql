CREATE TABLE `product_floor_line` (
  `product` varchar(100) NOT NULL,
  `floor` varchar(45) NOT NULL,
  `line` varchar(100) NOT NULL,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`product`,`floor`,`line`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
