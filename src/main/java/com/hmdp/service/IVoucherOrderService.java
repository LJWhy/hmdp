package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;


public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result createVoucherOrder(Long voucherId);

    Result seckillVoucher1(Long voucherId);

    void createVoucherOrder1(VoucherOrder voucherOrder);

    Result seckillVoucher2(Long voucherId);
}
