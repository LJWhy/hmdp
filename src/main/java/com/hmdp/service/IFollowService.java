package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Follow;
import com.baomidou.mybatisplus.extension.service.IService;


public interface IFollowService extends IService<Follow> {

    Result follow(Long followUserid, Boolean isFollow);

    Result isFollow(Long followUserId);

    Result followCommon(Long id);
}
