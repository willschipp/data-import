package com.emc.data.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/data/amqp")
public class AmqpFileController {

	private static final Log logger = LogFactory.getLog(AmqpFileController.class);
	
	@Autowired
	private RabbitTemplate rabbitTemplate;
	
	@Value("${routing.key:pcs.archive}")
	private String routingKey;

	
	@RequestMapping(method=RequestMethod.POST)
	public String uploadFile(@RequestParam("file") MultipartFile file) throws Exception {
		logger.info("received file: " + file);
		if (!file.isEmpty()) {
			byte[] bytes = file.getBytes();
			//split the file by line
			String totalFile = new String(bytes);
			//split
			MessageProperties properties = new MessageProperties();
			properties.setContentType("text/plain");
						
			String[] lines = totalFile.split("\n");
			for (String line : lines) {
				Message message = new Message(line.getBytes(),properties);
				rabbitTemplate.send(routingKey, message);
				logger.info("...sent");
			}//end for
			return "success";
		} else {
			return "failure";			
		}//end if
	}
}
