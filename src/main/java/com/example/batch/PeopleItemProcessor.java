package com.example.batch;

import com.example.model.People;
import com.example.model.PeopleDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class PeopleItemProcessor implements ItemProcessor<PeopleDTO, People> {
    private static final Logger log = LoggerFactory.getLogger(PeopleItemProcessor.class);

    @Override
    public People process(PeopleDTO item) throws Exception {
        if (item.getId() % 2 == 0 || "Male".equals(item.getSexo())) return null;
        People rslt = new People(item.getId(), item.getApellidos() + ", " + item.getNombre(),
                item.getCorreo(), item.getIp());
        log.info("Procesando: " + item);
        return rslt;
    }
}
