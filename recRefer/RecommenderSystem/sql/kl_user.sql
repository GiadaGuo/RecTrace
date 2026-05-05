-- ------------------------
-- ------- 用户信息 ---------
-- ------------------------

CREATE DATABASE IF NOT EXISTS `kl_user` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE kl_user;

DROP TABLE IF EXISTS `user_profile_behavior`;
CREATE TABLE IF NOT EXISTS `user_profile_behavior` (
   `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
   `user_id` BIGINT NOT NULL COMMENT '用户ID',
   `user_categories` VARCHAR(255) DEFAULT NULL COMMENT '用户爱好大类，分号分隔',
    `user_keywords` VARCHAR(255) DEFAULT NULL COMMENT '用户爱好细分，分号分隔',
    `age` INT DEFAULT NULL COMMENT '年龄',
    `gender` VARCHAR(1) NOT NULL COMMENT '性别：M/F',
    `user_click_last3m` INT NOT NULL DEFAULT 0 COMMENT '最近三个月点击数量',
    `user_cart_last3m` INT NOT NULL DEFAULT 0 COMMENT '最近三个月加购物车数量',
    `user_buy_last3m` INT NOT NULL DEFAULT 0 COMMENT '最近三个月购买数量',
    `user_forward_last3m` INT NOT NULL DEFAULT 0 COMMENT '最近三个月转发数量',
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='用户行为画像表';