-- ---------------------------
-- ----- 书籍信息和交互记录 ------
-- ---------------------------
CREATE DATABASE IF NOT EXISTS `kl_trade` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE kl_trade;

-- 书籍信息表
DROP TABLE IF EXISTS `book_info`;
CREATE TABLE IF NOT EXISTS `book_info` (
   `item_id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
   `seller_id` BIGINT NOT NULL COMMENT '卖家用户ID',
   `city` VARCHAR(100) DEFAULT '河南' COMMENT '书籍所在城市',
    `name` VARCHAR(100) NOT NULL COMMENT '书籍名称',
    `author` VARCHAR(100) NOT NULL COMMENT '作者',
    `publisher` VARCHAR(100) DEFAULT NULL COMMENT '出版社',
    `version` VARCHAR(100) DEFAULT NULL COMMENT '版本/版次',
    `price` DOUBLE NOT NULL COMMENT '价格',
    `type` VARCHAR(100) DEFAULT NULL COMMENT '类型（如教材/非教材）',
    `item_categories` VARCHAR(100) DEFAULT NULL COMMENT '一级分类',
    `item_keywords` VARCHAR(100) DEFAULT NULL COMMENT '二级分类',
    `note` BOOLEAN DEFAULT 1 COMMENT '是否有笔记',
    `description` TEXT DEFAULT NULL COMMENT '描述',
    `count` INT DEFAULT 1 COMMENT '数量',
    `status` INT NOT NULL DEFAULT 1 COMMENT '状态（1上架/0下架等）',
    `image` VARCHAR(255) DEFAULT NULL COMMENT '封面（URL）',
    `create_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`item_id`)
) ENGINE=InnoDB AUTO_INCREMENT=71 DEFAULT CHARSET=utf8mb4 COMMENT='书籍信息表';

-- 用户-商品交互行为表
DROP TABLE IF EXISTS `user_item_interaction`;
CREATE TABLE IF NOT EXISTS `user_item_interaction` (
   `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
   `user_id` BIGINT NOT NULL COMMENT '用户ID',
   `item_id` BIGINT NOT NULL COMMENT '商品/书籍ID',
   `rating` INT NOT NULL DEFAULT 1 COMMENT '兴趣等级，等于后面四项的和',
   `create_time` DATETIME NOT NULL COMMENT '交互时间',
   `hour` TINYINT NOT NULL COMMENT '小时(0-23)',
   `is_weekend` BOOLEAN NOT NULL DEFAULT 0 COMMENT '是否周末:0否1是',
   `is_holiday` BOOLEAN NOT NULL DEFAULT 0 COMMENT '是否节假日:0否1是',
   `click` BOOLEAN NOT NULL DEFAULT 1 COMMENT '是否点击 -> 默认为1，作为后面三项的前提行为',
   `cart` BOOLEAN NOT NULL DEFAULT 0 COMMENT '是否加购',
   `forward` BOOLEAN NOT NULL DEFAULT 0 COMMENT '是否转发',
   `buy` BOOLEAN NOT NULL DEFAULT 0 COMMENT '是否购买',
   PRIMARY KEY (id),
    KEY idx_user_id (user_id),
    KEY idx_item_id (item_id),
    KEY idx_datetime (datetime),
    KEY idx_user_item_time (user_id, item_id, datetime),
    FOREIGN KEY (item_id) REFERENCES book_info(`item_id`)
    ON DELETE CASCADE -- 当 book_info 表记录被删除时，关联的交互记录也删除
    ON UPDATE CASCADE -- 当 book_info 表 item_id 被更新时，关联的 item_id 也更新
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户-商品交互行为表';

-- 商品-用户交互行为统计表
DROP TABLE IF EXISTS `item_interaction`;
CREATE TABLE IF NOT EXISTS `item_interaction` (
      `item_id` BIGINT NOT NULL COMMENT '商品信息ID（唯一）',
      `item_click_last3m` INT NOT NULL DEFAULT 0 COMMENT '最近3个月点击次数',
      `item_cart_last3m` INT NOT NULL DEFAULT 0 COMMENT '最近3个月加购次数',
      `item_buy_last3m` INT NOT NULL DEFAULT 0 COMMENT '最近3个月购买次数',
      `item_forward_last3m` INT NOT NULL DEFAULT 0 COMMENT '最近3个月转发次数',
      `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
      `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
      PRIMARY KEY (item_id),
    FOREIGN KEY (item_id) REFERENCES book_info(`item_id`)
    ON DELETE CASCADE -- 当 book_info 表记录被删除时，关联的交互记录也删除
    ON UPDATE CASCADE -- 当 book_info 表 item_id 被更新时，关联的 item_id 也更新
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品-用户交互行为统计表';






